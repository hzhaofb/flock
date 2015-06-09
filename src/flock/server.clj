;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 4/16/15
;; flock.server
;; 
;; Purpose: represent the server instance
;;   Serves as a keeper of the server slot number
;;   which is used during task reservation
;;   to calculate which task id to reserve.
;;   Update the server heartbeat periodically.
;;   Clean up when server goes away and refresh the server.
;;   Hold state {:sid <server-slot-id> :sid-list <current sorted list of sid> :

(ns flock.server
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [base.util :as util]
            [com.stuartsierra.component :as component]
            [component.database :refer [mydb insert-row]]
            [component.core :refer [get-config-int]]
            [component.scheduler :refer [schedule-fixed-delay]])
  (:import (java.sql SQLException)))

; public API
(defn get-slot-info
  "public API that keep the server-slot id
   Calculated by the current server's index position in the sorted current server list.
   Returns {:slot <slot-number> :server-count <count of active servers>"
  [comp]
  (let [state (:state comp)
        sid (:sid @state)
        sid-list (:sid-list @state)]
    {:slot (util/index-of sid sid-list)
     :server-count (count sid-list)}))

(defn refresh-sid-list
  [comp]
  (let [state (:state comp)
        sid (:sid @state)
        curr-sids (:sid-list @state)
        new-sids (->> "select sid from server order by sid"
                      (jdbc/query (mydb comp))
                      (map :sid))]
    (when (not= new-sids curr-sids)
      (swap! state assoc :sid-list new-sids)
      (log/info "server" sid ": server list change from " curr-sids
                "to" new-sids "slot-info" (get-slot-info comp)))))

(defn- insert-or-get-server-id
  [comp]
  (let [ip (util/get-ip)
        pid (util/get-pid)]
    (try
      (insert-row (mydb comp) :server {:ip ip :pid pid})
      (catch SQLException ex
        (-> (jdbc/query (mydb comp) ["select sid from server where ip=? and pid=?" ip pid])
            (first)
            (:sid))))))

(defn- register-server
  "Called when a new server starts.
  Create a server record using current which is unique.
  returns server created"
  ([comp]
   (let [sid (insert-or-get-server-id comp)]
     (log/info "registered server sid" sid)
     (swap! (:state comp) assoc :sid sid)
     (refresh-sid-list comp))))

(defn update-server-heartbeat
  "update server heartbeat"
  [comp]
  (let [sid (-> (:state comp)
                (deref)
                (:sid))
        _ (log/debug "update server heatbeat" sid)
        updated (->> ["update server set heartbeat = current_timestamp() where sid = ?" sid]
                     (jdbc/execute! (mydb comp))
                     (first))]
    (when (= updated 0)
      (log/warn "server" sid "heartbeat failed. Server was presumed dead, reregister")
      (register-server comp)))
  (refresh-sid-list comp))

(defn- monitor-dead-server
  "Called by monit-thread to delete dead servers"
  [comp]
  (try
    (let [heartbeat (get-config-int comp "flock.server.heartbeat" 10)
          max-skip (get-config-int comp "flock.server.max.skipped.heartbeats" 4)
          allowance (* heartbeat max-skip)
          _ (log/debug "delete server with heartbeat" allowance "sec old")
          delete_count (->> ["heartbeat < now() - INTERVAL ? SECOND" allowance]
                            (jdbc/delete! (mydb comp) :server)
                            (first))]
      (if (> delete_count 0)
        (do
          (log/info "deleted" delete_count "dead servers")
          (refresh-sid-list comp))))
    (catch Exception ex
      (log/error ex "check dead server error"))))

(defn start-server
  "start this server and schedule maintenance task for heartbeat and dead server."
  [comp]
  (let [comp (assoc comp :state (atom {}))
        scheduler (:scheduler comp)
        cycle (get-config-int comp "flock.server.monitor.cycle.sec" 10)
        heartbeat (get-config-int comp "flock.server.heartbeat" 10)]
    (register-server comp)
    (log/info "starting server heartbeat" heartbeat "sec")
    (schedule-fixed-delay scheduler {:command (fn [] (update-server-heartbeat comp)) :delay heartbeat})
    (log/info "starting server monitoring, checking every" cycle "sec")
    (schedule-fixed-delay scheduler {:command (fn [] (monitor-dead-server comp)) :delay cycle})
    comp))

; Domain model component for
(defrecord ServerComponent [core flock-db]
  component/Lifecycle
  (start [this]
    (start-server this))

  (stop [this]
    (dissoc this :state)))

(defn new-server-comp
  []
  (map->ServerComponent {}))
