;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 1/5/15
;; component.graphite
;; 
;; Purpose: report metrix to graphite.
;; Just include this component in system map,
;; the metrics will be collected and sent to service.config graphite.tcp.host
;;

(ns component.graphite
  (:require [com.stuartsierra.component :as component]
            [metrics.reporters.graphite :as mg]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [component.core :refer [cfg]]
            [base.util :as util])
  (:import (com.codahale.metrics.graphite Graphite)))

(defn send-raw-metrics
  "send metrics directly to graphite server as configured
   metrics key: e.g. flock.task.upcoming-5days
   val-timestamp-list: list of map {:val val :timestamp timestamp}
    timestamp in epoch"
  [graphite-comp metrics-key val-timestamp-list]
  (let [{host :host port :port prefix :prefix}
        (:config graphite-comp)
        g (Graphite. host port)
        fullkey (str prefix "." metrics-key)]
    (when host
      (.connect g)
      (doseq [{val :val timestamp :timestamp} val-timestamp-list]
        (.send g fullkey (str val) timestamp))
      (.close g))))

(defrecord GraphiteReporter [core]
  component/Lifecycle

  (start [this]
    (let [host-port
          (-> (:config core)
              (deref)
              (get "graphite.tcp.host"))
          [host port] (some-> host-port (str/split #"\:"))
          port (some-> port (util/to-int))
          opts (when host {:host host :port port :prefix "flock"})
          reporter (when opts (mg/reporter opts))]
      (if reporter
        (do
          (mg/start reporter 20)
          (log/info "Started graphite reporter on " opts)
          (assoc this :reporter reporter :config opts))
        (do
          (log/info "graphite.tcp.host is not configured. Disable graphite reporting")))))

  (stop [this]
    (log/info "Stopping graphite component")
    (some-> (:reporter this)
            (mg/stop))))

(defn new-graphite-reporter
  []
  (map->GraphiteReporter {}))

