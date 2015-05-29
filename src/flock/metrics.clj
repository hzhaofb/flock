;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 5/20/15
;; flock.metrics
;; 
;; Purpose: declare and send flock metrics
;;

(ns flock.metrics
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [metrics.timers :refer [timer time!]]
            [metrics.histograms :refer [histogram update!]]
            [metrics.meters :refer [meter mark!]]
            [metrics.gauges :refer [gauge-fn]]
            [component.core :refer [cfg]]
            [flock.util :refer [myreplicadb get-config-int]]
            [flock.environment :as env]
            [base.util :as util]
            [component.graphite :as gcomp]
            [component.scheduler :as scheduler]))

; holding a map from wid to slack of that worker
; slack is the difference between reserve (ask) and response task list size
(def worker-slack (atom {}))

(defn update-worker-slack!
  [wid slack]
  (swap! worker-slack assoc wid slack))

(defn- send-worker-slack
  "returns worker slack count and reset the map.
  For this to have acurate count, worker must call reserve_tasks within the
  Graphite pulling period, currently 20 secs"
  []
  (let [slack (reduce + 0 (vals @worker-slack))]
    ; need to reset the slack between calls so expired workers are not counted.
    ; if a worker didn't reserve_task during the graphite interval, then it has no slack
    (log/info "report worker slack" slack @worker-slack)
    (reset! worker-slack {})
    slack))

(defn- count-task-backlog
  [comp]
  (-> (jdbc/query
        (myreplicadb comp)
        "select count(*) as backlog from schedule
                where eta < unix_timestamp(now())
                  and wid = 0")
      (first)
      (:backlog)))



;;; define metrics for task. Note, we use def form instead
;;; of macro so intellij syntax look better.
(def reserved-meter (meter ["flock" "task" "reserved-meter"]))

(def completed-meter (meter ["flock" "task" "completed-meter"]))

(def error-meter (meter ["flock" "task" "error-meter"]))

(def queuing-time (histogram ["flock" "task" "queuing-time"]))

(def processing-time (histogram ["flock" "task" "processing-time"]))

(def complete-db-timer (timer ["flock" "task" "complete-db-timer"]))

(def reserve-db-timer (timer ["flock" "task" "reserve-db-timer"]))

; create gauge for total backlog

(defn send-graphite-future-task-counts
  [comp]
  (try
    (let [eids (util/get-int-list (cfg comp) "flock.task.metrics.future-count-eids")
          days (get-config-int comp "flock.task.metrics.future-count-days" 5)
          interval (get-config-int comp "flock.task.metrics.future-count-interval-secs" 5)
          envs (-> (env/get-envs (get comp :env-comp))
                   (select-keys eids))
          g (get comp :graphite)
          cutoff (+ (util/current-epoch) (* days 86400))]
      (doseq [[eid env] envs]
        ; send-raw-metrics require a va
        (let [sql "select count(tid) as val, floor((eta/?) * ?) as timestamp
                  from schedule
                  where eid=? and wid=0 and eta < ? group by timestamp"
              q [sql interval interval eid cutoff]
              mlist (jdbc/query (myreplicadb comp) q)
              key (str "flock.task." env "-upcoming-" days "days")]
          (log/info "sending metrics" key "count:" (count mlist) (take 10 mlist))
          (gcomp/send-raw-metrics g key mlist))))
    (catch Exception ex
      (log/error ex "failed to send graphite tasks"))))

(defn- start-metrics
  "start all metrics"
  [comp]
  (gauge-fn ["flock" "task" "task-backlog-gauge"]
            #(int (count-task-backlog comp)))
  (gauge-fn ["flock" "task" "worker-slack-gauge"]
            send-worker-slack)
  (let [future-task-report-cycle
        (get-config-int comp "flock.task.metrics.future-report-secs" 60)
        report-fn
        (fn [] (send-graphite-future-task-counts comp))
        scheduler (get comp :scheduler)]
    (scheduler/schedule-fixed-delay
      scheduler {:command report-fn
                 :delay future-task-report-cycle})
    (log/info "scheduled to send future task metrics every"
              future-task-report-cycle "secs")))

; Declare dependencies
(defrecord MetricsComponent [core flock-db flockreplica-db
                             graphite scheduler
                             env-comp]
  component/Lifecycle
  (start [this]
    (start-metrics this)
    this)

  (stop [this]
    this))

(defn new-metrics-comp
  []
  (map->MetricsComponent {}))
