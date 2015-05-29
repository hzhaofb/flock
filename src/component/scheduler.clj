;; -*- coding: utf-8 -*-
;; Author: Howard Zhao
;; created: 3/25/15
;; component.scheduler
;;
;; Purpose: encapsulate Java scheduled executor for periodical command execution
;; in thread pool
;;

(ns component.scheduler
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component])
  (:import (java.util.concurrent Executors TimeUnit)))

(defn schedule-fixed-delay
  "schedule execution of command with fixed delay of delay time_unit
  (default TimeUnit/SECOND) with initial delay (default 0)"
  [comp {command :command init_delay :init_delay time_unit :time_unit delay :delay
         :or   {time_unit TimeUnit/SECONDS init_delay 0}}]
  (-> (get comp :scheduled_threadpool)
      (.scheduleWithFixedDelay command init_delay delay time_unit)))

(defn schedule-fixed-rate
  "schedule execution of command every fixed delay time_unit (default TimeUnit/SECOND)
  with initial delay (default 0)"
  [comp {command :command init_delay :init_delay time_unit :time_unit period :period
         :or   {time_unit TimeUnit/SECONDS init_delay 0}}]
  (-> (get comp :scheduled_threadpool)
      (.scheduleWithFixedRate command init_delay period time_unit)))

(defrecord SchedulerComponent []
  component/Lifecycle

  (start [this]
    (log/info "starting scheduler component")
    (let [scheduler (Executors/newScheduledThreadPool 1)]
      (assoc this :scheduled_threadpool scheduler)))

  (stop [this]
    (log/info "stopping scheduler thread pool")
    (.shutdown (get this :scheduled_threadpool))
    (dissoc this :scheduled_threadpool)))

(defn new-scheduler
  []
  (map->SchedulerComponent {}))