;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; flock application top-level system specification

(ns flock.system
  (:require
    [com.stuartsierra.component :as component]
    [component.core :as core]
    [component.http-kit :as http-kit]
    [component.health :as health]
    [component.graphite :as graphite]
    [component.database :as database]
    [component.nrepl :as nrepl]
    [component.scheduler :as scheduler]
    [flock.server :as server]
    [flock.tasklog :as tasklog]
    [flock.metrics :as metrics]
    [flock.environment :as environment]
    [flock.task :as task]
    [flock.func :as func]
    [flock.worker :as worker]))

(defn prod-system []
  (component/system-map
    :core           (core/new-core "flock")
    :graphite       (component/using (graphite/new-graphite-reporter)
                                     [:core])
    :scheduler      (component/using (scheduler/new-scheduler)
                                     [:core])
    :flock-db       (component/using (database/new-database "flock")
                                     [:core])
    :flockreplica-db (component/using (database/new-database "flockreplica")
                                     [:core])
    :flocklog-db    (component/using (database/new-database "flocklog")
                                     [:core])
    :server-comp    (component/using (server/new-server-comp)
                                     [:core
                                      :scheduler
                                      :flock-db])
    :env-comp        (component/using (environment/new-env-comp)
                                      [:core
                                       :flock-db])
    :metrics-comp    (component/using (metrics/new-metrics-comp)
                                      [:core
                                       :flock-db
                                       :flockreplica-db
                                       :env-comp
                                       :graphite
                                       :scheduler])
    :tasklog-comp    (component/using (tasklog/new-tasklog-comp)
                                      [:core
                                       :flocklog-db])
    :worker-comp     (component/using (worker/new-worker-comp)
                                     [:core
                                      :scheduler
                                      :flock-db
                                      :tasklog-comp
                                      :env-comp])
    :func-comp       (component/using (func/new-func-comp)
                                     [:core
                                      :flock-db
                                      :env-comp])
    :task-comp       (component/using (task/new-task-comp)
                                     [:core
                                      :flock-db
                                      :server-comp
                                      :tasklog-comp
                                      :worker-comp
                                      :func-comp
                                      :flockreplica-db
                                      :scheduler
                                      :graphite
                                      :env-comp])
    :health-service (component/using (health/new-health-service)
                                     [:core])
    :http-server    (component/using (http-kit/new-web-server)
                                     [:core
                                      :health-service
                                      :tasklog-comp
                                      :worker-comp
                                      :func-comp
                                      :task-comp
                                      ])))

(defn dev-system []
  ; current no difference from prod-system.
  ; can override with assoc if need to change the commponent config
  (-> (prod-system)
      (assoc :core (core/new-core "flocktest"))))

