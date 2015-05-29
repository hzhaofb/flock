;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; component to create and access tasklog

(ns flock.tasklog
  (:require [compojure.core :as cc]
            [clojure.java.jdbc :as jdbc]
            [clj-time.core :as tc]
            [clj-time.coerce :as tcc]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [ring.middleware.json :refer [wrap-json-body
                                          wrap-json-params
                                          wrap-json-response]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [component.webservice :refer [WebService]]
            [flock.util :refer [mylogdb get-config-int]]
            [base.rest-util :refer [json-response echo]]
            [base.util :refer :all]
            [component.core :refer [cfg]]))

(def type-to-config-key
  {"S" "flock.log.task.start"
   "C" "flock.log.task.complete"
   "X" "flock.log.task.expire"})

(defn write-tasklog
  "write a task log including [:tid :eta :new_eta :start_time :event_type :wid :error]"
  [comp tlog]
  (let [ltype (:event_type tlog)
        write (->> ltype
                   type-to-config-key
                   (get (cfg comp)))]
    (if (= "true" write)
      (->> (select-keys tlog [:tid :event_type :eta :new_eta :start_time :wid :error])
           (jdbc/insert! (mylogdb comp) :task_log)))))


(defn list-tasklog
  "List all task log for task tid order by log time desc"
  [comp tid]
  (->> ["select * from task_log where tid=? order by log_time desc" tid]
       (jdbc/query (mylogdb comp))
       (doall)
       (assoc {} :logs)))

(defn- make-routes [tasklog-comp]
  (cc/routes
    (cc/GET "/tasklog/:tid" [tid :as req]
            ;; list log for a given task id
            (json-response req list-tasklog tasklog-comp tid))

    (cc/POST "/ptasklog" req
             ;; post on this endpoint for sanity test
             (json-response req echo req))
    ))

(defrecord TaskLogComponent [core flocklog-db]
  component/Lifecycle
  (start [this]
    (log/info "Starting TaskLog Service")
    (->> (make-routes this)
         (wrap-json-response)
         (assoc this :routes)))

  (stop [this]
    this)

  WebService
  (get-routes [this]
    (:routes
     this)))

(defn new-tasklog-comp
  []
  (map->TaskLogComponent {}))
