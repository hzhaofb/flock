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
    [component.database :as database]
    [component.scheduler :as scheduler]
    [flock.server :as server]
    [flock.environment :as environment]
    [flock.task :as task]
    [flock.func :as func]
    [flock.worker :as worker]))

(defn prod-system []
  (component/system-map
    :core           (core/new-core "flock")
    :scheduler      (component/using (scheduler/new-scheduler)
                                     [:core])
    :flock-db       (component/using (database/new-database "flock")
                                     [:core])
    :server-comp    (component/using (server/new-server-comp)
                                     [:core
                                      :scheduler
                                      :flock-db])
    :env-comp        (component/using (environment/new-env-comp)
                                      [:core
                                       :flock-db])
    :worker-comp     (component/using (worker/new-worker-comp)
                                     [:core
                                      :scheduler
                                      :flock-db
                                      :env-comp])
    :func-comp       (component/using (func/new-func-comp)
                                     [:core
                                      :flock-db
                                      :env-comp])
    :task-comp       (component/using (task/new-task-comp)
                                     [:core
                                      :flock-db
                                      :server-comp
                                      :worker-comp
                                      :func-comp
                                      :scheduler
                                      :env-comp])
    :health-service (component/using (health/new-health-service)
                                     [:core])
    :http-server    (component/using (http-kit/new-web-server)
                                     [:core
                                      :health-service
                                      :worker-comp
                                      :func-comp
                                      :task-comp
                                      ])))

(defn dev-system []
  ; current no difference from prod-system.
  ; can override with assoc if need to change the commponent config
  (-> (prod-system)
      (assoc :core (core/new-core "flocktest"))))

