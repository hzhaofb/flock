;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; Component to manage the worker instances including
;; checking for expired workers.

(ns flock.worker
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [clojure.string :refer [join]]
            [base.util :as util]
            [component.scheduler :refer [schedule-fixed-delay]]
            [flock.environment :refer [get-env-by-id get-env-by-name]]
            [component.database :refer [mydb get-conn get-single-row insert-row]]
            [component.core :refer [get-config-int]]
            [com.stuartsierra.component :as component])
  (:import (java.sql SQLException)))

(defn- convert-heartbeat [worker]
  (if-let [hb (:heartbeat worker)]
    (assoc worker :heartbeat (. hb getTime))
    worker))

(defn- convert-env [comp worker]
  (->>  (:eid worker)
        (get-env-by-id (get comp :env-comp))
        (assoc worker :env)))

(defn- before-response
  "some processing of worker map before returning to client"
  [comp worker]
  (let [worker (convert-heartbeat worker)
        worker (convert-env comp worker)]
    worker))

(defn get-worker-by-id
  [comp wid]
  (if-let [worker (get-single-row (mydb comp) :worker :wid wid)]
    (before-response comp worker)))

(defn get-worker-by-ip-pid
  [comp ip pid]
  (let [worker (-> (mydb comp)
                   (jdbc/query ["select * from worker where ip=? and pid=?" ip pid])
                   (first))]
    (before-response comp worker)))

(defn start-worker
  "Called when a new worker starts.
  Create a worker record using ip address and process pid which is unique.
  returns worker created"
  [comp ip pid env]
  (assert (some? ip) "Invalid ip")
  (assert (some? pid) "Invalid pid")
  (let [pid (util/to-int pid)
        _ (assert (pos? pid))
        db (mydb comp)
        eid (get-env-by-name (get comp :env-comp) env)]
    (assert (some? env) "Invalid env")
    (try
      (let [wid (insert-row db :worker {:ip ip :pid pid :eid eid})
            worker (get-single-row db :worker :wid wid)]
        (insert-row db :worker_log (assoc worker :event "START"))
        (log/info "created worker" worker)
        (before-response comp worker))
      (catch SQLException ex
        (log/info "Worker at ip=" ip "pid=" pid "alreay registered")
        (-> (get-worker-by-ip-pid comp ip pid)
            (assoc :msg "worker already registered"))))))

(defn set-admin-cmd
  "update worker admin command"
  [comp wid cmd]
  (if (= '(0) (jdbc/update! (mydb comp)
                            :worker {:admin_cmd cmd} ["wid=?" wid]))
    {:msg (str "worker " wid " is not found")}
    (let [db (mydb comp)
          worker (get-single-row db :worker :wid wid)]
      (log/info "Worker " wid " new admin cmd=" cmd)
      (insert-row db :worker_log (assoc worker :event "ADMIN_CMD"))
      (before-response comp worker))))

(defn update-heartbeat
  "update worker heartbeat and status.
  returns updated worker, including admin_cmd"
  [comp wid wstatus]
  (if (= '(0) (jdbc/execute!
                (mydb comp)
                ["update worker set wstatus = ?,
                  heartbeat = current_timestamp() where wid = ?" wstatus wid]))
    {:msg (str "worker " wid " is not found")
     :admin_cmd "SHUTDOWN"}
    (do (log/info "Worker" wid "updated wstatus " wstatus " and heartbeat")
        (get-worker-by-id comp wid))))

(defn- release-task
  "complete a task because worker failed to update heartbeat, task in form of {:tid tid :eta eta}"
  [comp {tid :tid wid :wid rid :rid}]
  (jdbc/update! (mydb comp) :schedule {:wid 0} [ "tid=? and wid=?" tid wid])
  (log/info "expire task" tid "wid" wid))

(defn cleanup-worker
  "Called when worker shutdown or presumed dead (indicated by event).
  Release reserved tasks and remove worker row.
  returns (1) for worker is clean up, (0) for worker is not active"
  [comp event worker]
  (log/info "cleanup worker" worker event)
  (let [db (mydb comp)
        wid  (worker :wid)
        sql ["select tid, eta, wid as wid from schedule where wid in (?, ?)" wid (- 0 wid)]
        tasks (jdbc/query db sql)
        _ (doall (map (partial release-task comp) tasks))
        tids (map :tid tasks)
        tids-str (-> (interpose "," tids)
                     (join)
                     (util/trunc 1000))
        worker-log (assoc worker :event event :tids tids-str)]
    (jdbc/insert! db :worker_log worker-log)
    (jdbc/delete! db :worker ["wid=?" wid])
    (jdbc/update! db :schedule {:wid 0} ["wid in (?, ?)" wid (- 0 wid)])))

(defn stop-worker [comp wid]
  (let [db (mydb comp)]
    (if-let [worker (get-single-row db :worker "wid" wid)]
      (do (cleanup-worker comp "SHUTDOWN" worker)
          {:msg (str "worker " wid " stopped")})
      {:msg "worker is already dead"})))

(defn- check-dead-workers
  "check for dead workers and clean them up if any."
  [comp]
  (try
    (let [db (mydb comp)
          heartbeat (get-config-int comp "flock.worker.heartbeat" 5)
          max-skip (get-config-int comp "flock.worker.max.skipped.heartbeats" 4)
          allowance (* heartbeat max-skip)]
      (log/info "checking for dead worker with heartbeat older than" allowance "secs")
      (doall
        (->> ["select * from worker where heartbeat < now() - INTERVAL ? SECOND" allowance]
             (jdbc/query db)
             (map #(cleanup-worker comp "EXPIRED" %)))))
    (catch Exception ex
      (log/error ex "check dead worker error"))))

(defn start-monitor
  "start monitor thread that look for dead workers."
  [comp]
  (log/info "start monitoring worker using scheduler")
  (let [monitor-cycle (get-config-int comp "flock.worker.monitor.cycle.sec" 10)
        monit_worker (fn [] (check-dead-workers comp))
        scheduler (get comp :scheduler)]
    (schedule-fixed-delay scheduler {:command monit_worker :delay monitor-cycle}))
  comp)

(defn list-worker-log [comp wid]
  {:logs
    (->> ["select * from worker_log where wid=? order by event_time" wid]
       (jdbc/query (mydb comp))
       (map #(before-response comp %)))})

; Domain model component for workers
(defrecord WorkerComponent [core scheduler flock-db env-comp]
  component/Lifecycle
  (start [this]
    (start-monitor this))

  (stop [this]
    this))

(defn new-worker-comp
  []
  (map->WorkerComponent {}))
