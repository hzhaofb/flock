;; -*- coding: utf-8 -*-
;; (c)2014 Flipboard Inc, All Rights Reserved.
;; Author: Howard Zhao
;; created: 6/8/15
;; flock.rest_api
;; 
;; Purpose: expose rest api endpoints for all services provided by flock
;;

(ns flock.rest-api
  (:require [com.stuartsierra.component :as component]
            [ring.middleware.json :refer [wrap-json-body wrap-json-params wrap-json-response]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [org.httpkit.server :refer [run-server]]
            [clojure.tools.logging :as log]
            [compojure.core :as cc]
            [base.rest-util :refer [json-response echo]]
            [component.core :as core]
            [flock.func :as func]
            [flock.task :as task]
            [flock.worker :as worker]))


(defn- wrap-common-middleware
  [routes]
  (-> routes
      (wrap-json-response)
      (wrap-keyword-params)
      (wrap-params)
      ; following only apply if request content-type is json
      (wrap-json-params)
      (wrap-content-type)))

(defn- get-routes
  "assemble all of the components that implement the WebService protocol
  into an application map (in the order given)"
  [this]
  (let [func-comp (get this :func-comp)
        task-comp (get this :task-comp)
        worker-comp (get this :worker-comp)]
    (cc/routes
      (cc/POST "/echo" req
        ;; post on this endpoint for sanity test
        (json-response req echo req))

      ;func related
      (cc/GET "/func/:fid" [fid :as req]
        ;; get function with id
        (json-response req func/get-func func-comp fid))
      (cc/POST "/func" req
        ;; create a new function with specified
        ;; eid, name, and settings
        ;; if already exist, return existing func
        (json-response req func/create-func func-comp (req :params)))
      (cc/PUT "/func/:fid" [fid settings :as req]
        ;; update function's settings
        (json-response req func/update-func func-comp fid settings))


      ; task related
      (cc/GET "/worker/:wid/task" [wid :as req]
        ;; reserves a task for a given worker wid.
        ;; worker must PUT back with new_eta after complete
        (json-response req task/reserve-task task-comp wid))
      (cc/GET "/worker/:wid/tasks" [wid limit :as req]
        ;; reserves tasks upto the limit for a given worker wid.
        ;; worker must PUT back each task with new_eta after complete
        (json-response req task/reserve-tasks task-comp wid limit))
      (cc/PUT "/worker/:wid/task/:tid" [wid tid new_eta error :as req]
        ;; report task complete with new eta.
        (json-response req task/complete-task task-comp (req :params)))
      (cc/PUT "/task/cache" [:as req]
        ;; clear task candidate list cache
        (json-response req task/reset-task-cache task-comp))
      (cc/GET "/task/:tid" [tid :as req]
        ;; get task with id tid
        (json-response req task/get-task-by-id task-comp tid))
      (cc/DELETE "/task/:tid" [tid :as req]
        ;; delete task with id tid
        (json-response req task/delete-task task-comp tid))
      (cc/GET "/task" [fid task_key :as req]
        ;; get task with given fid and task_key as query params
        (json-response req task/get-task-by-fid-key task-comp fid task_key))
      (cc/POST "/task" req
        ;; create a new task
        (json-response req task/create-task task-comp (req :params)))
      (cc/PUT "/task/:tid" [tid task_key eta params :as req]
        ;; update existing task
        (json-response req task/update-task task-comp tid task_key eta params))
      (cc/GET "/func/:fid/backlog" [fid :as req]
        ;; get function with id
        (json-response req task/list-func-backlog task-comp fid))
      (cc/GET "/func/:fid/backlog_count" [fid :as req]
        ;; get function with id
        (json-response req task/count-func-backlog task-comp fid))

      ; worker related
      (cc/POST "/worker" [ip pid env :as req]
        ;; create a new worker with specified ip and pid
        ;; if already exist, return existing workerId wid
        (json-response req worker/start-worker worker-comp ip pid env))
      (cc/PUT "/worker/:wid" [wid wstatus :as req]
        ;; report status for a given worker and update heartbeat
        (json-response req worker/update-heartbeat worker-comp wid wstatus))
      (cc/PUT "/worker/:wid/admin" [wid cmd :as req]
        ;; set admin command of the worker
        (json-response req worker/set-admin-cmd worker-comp wid cmd))
      (cc/DELETE "/worker/:wid" [wid :as req]
        ;; report worker shutdown
        (json-response req worker/stop-worker worker-comp wid))
      (cc/GET "/worker/:wid" [wid :as req]
        ;; get all details of worker
        (json-response req worker/get-worker-by-id worker-comp wid))
      (cc/GET "/worker/ip_pid/:ip/:pid" [ip pid :as req]
        ;; get all details of worker
        (json-response req worker/get-worker-by-ip-pid worker-comp ip pid))
      (cc/GET "/worker_log/:wid" [wid :as req]
        ;; all worker log for wid
        (json-response req worker/list-worker-log worker-comp wid)))))

(defrecord RestAPI []
  component/Lifecycle

  (start [this]
    (let [port (core/get-config-int this "flock.http.port" 8080)
          app (-> (get-routes this)
                  (wrap-common-middleware))
          server (run-server app {:port port :join? false})]
      (log/info "Started flock rest-api service on port" port)
      (assoc this :server server :port port)))

  (stop [this]
    (log/info "stopping flock rest-api component")
    (if-let [stop-server (:server this)]
      (stop-server))
    this))

(defn new-rest-api
  []
  (map->RestAPI {}))
