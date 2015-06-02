;; -*- coding: utf-8 -*-
;; Author: Howard Zhao
;; Purpose: renders admin pages
;; TODO remove

(ns flock.admin
  (:require [com.stuartsierra.component :as component]
            [component.webservice :refer [WebService]]
            [compojure.core :as cc]
            [compojure.core :as cc]
            [ring.util.response :as rr]
            [flock.task :as tc]
            [flock.util :refer [myreplicadb mylogdb]]
            [base.mysql :refer [join-table]]
            [base.html-util :refer [html-response]]
            [clojure.java.jdbc :as jdbc]
            [base.util :as util]
            [clojure.string :refer [join]]))

(defn- resolve-id-to-name
  "Given a list of maps with id, add name of the id from database table to the map.
  e.g., specs: {:id :eid :table \"environment\" :column :name :dest-key :env}"
  [{db :db id :id table :table column :column dest-key :dest-key} xs]
  (if (id (first xs))
    (let [lookup
          ; maps the id to the corresponding results from sql
          (->> (map id xs)
               (distinct)
               (join ",")
               (format (str "select " (name column) ", " (name id)
                            " from " table " where " (name id) " in (%s)") )
               (jdbc/query db)
               (reduce #(assoc %1 (id %2) %2) {}))]
      (map #(assoc % dest-key (get-in lookup [(id %) column])) xs))
    xs))

(defn- resolve-ids
  "resolves following eid, fid, rid to their corresponding names"
  [comp xs]
  (let [db (myreplicadb comp)] (->> xs
        (resolve-id-to-name
          {:db db :id :eid :table "environment"
           :column :name :dest-key :env})
        (resolve-id-to-name
          {:db db :id :fid :table "func"
           :column :name :dest-key :function}))))

(def page-size 50)

(defn paging-query
  [comp paging query & args]
  (let [start (-> (get paging :start)
                  (util/to-int-default 0))
        psize (-> (get paging :page_size)
                  (util/to-int-default page-size))
        db (myreplicadb comp)
        q (str query " limit ?,?")]
    (->> (concat [q] args [start (+ start psize)])
         (jdbc/query db)
         (resolve-ids comp))))

(defn workers
  [comp paging]
  {:workers
   (paging-query comp paging "select * from worker")
   :logs
   (paging-query comp paging "select * from worker_log")})

(defn environments
  [comp paging]
  {:environments
   (paging-query comp paging "select * from environment")})

(defn worker
  [comp wid]
  (let [wid  (util/to-int wid)]
    {:worker
     (->> ["select * from worker where wid=?" wid]
          (jdbc/query (myreplicadb comp))
          (resolve-ids comp))
     :worker-log
     (->>  ["select * from worker_log where wid=?" wid]
           (jdbc/query (myreplicadb comp))
           (resolve-ids comp))}))

(defn- add-task-schedule
  "Given a task table result, add corresponding schedule info."
  [comp tasks]
  (join-table (myreplicadb comp) "schedule" :tid tasks))

(defn- add-task-detail
  "Given a schedule table result, add corresponding task info."
  [comp tschedule]
  (join-table (myreplicadb comp) "task" :tid tschedule))

(defn functions
  [comp paging]
  {:functions
   (paging-query comp paging "select * from func")})

(defn function
  [comp fid]
  (let [fid (util/to-int fid)]
    {:function
     (->> ["select * from func where fid=?" fid]
          (jdbc/query (myreplicadb comp))
          (resolve-ids comp))
     :tasks
     (->> (paging-query comp {} "select * from task where fid=?" fid)
          (map tc/from-db)
          (add-task-schedule comp)
          (resolve-ids comp))}))

(defn tasks
  [comp start]
  (let [start (util/to-int-default start 0)]
    {:running
     (->> ["select * from task t join
          (select * from schedule where wid > 0 order by eta limit ?,?) s
          on s.tid=t.tid order by eta" start (+ start 50)]
         (jdbc/query (myreplicadb comp))
         (map tc/from-db)
         (resolve-ids comp))
     :pending
     (->> ["select * from task t join
          (select * from schedule s where wid = 0 order by eta limit ?,?) s
          on s.tid=t.tid order by eta" start (+ start 50)]
         (jdbc/query (myreplicadb comp))
         (map tc/from-db)
         (resolve-ids comp))}))

(defn task
  [comp tid]
  (let [tid (util/to-int tid)]
    {:task
     (->> ["select * from task where tid=?" tid]
          (jdbc/query (myreplicadb comp))
          (map tc/from-db)
          (add-task-schedule comp)
          (resolve-ids comp))
     :task-logs
     (->> ["select * from task_log where tid=? order by log_time desc" tid]
          (jdbc/query (mylogdb comp))
          (resolve-ids comp))}))

(defn- make-routes
  [comp]
  (cc/routes
    (cc/GET "/" []
            (rr/redirect "/admin/workers"))
    (cc/GET "/admin/workers" [start]
            (html-response "workers.html" workers comp start))
    (cc/GET "/admin/worker/:wid" [wid]
            (html-response "worker.html" worker comp wid))
    (cc/GET "/admin/environments" [start]
            (html-response "environments.html" environments comp start))
    (cc/GET "/admin/worker/:wid" [wid]
            (html-response "worker.html" worker comp wid))
    (cc/GET "/admin/tasks" [start]
            (html-response "tasks.html" tasks comp start))
    (cc/GET "/admin/task/:tid" [tid]
            (html-response "task.html" task comp tid))
    (cc/GET "/admin/functions" [start]
            (html-response "functions.html" functions comp start))
    (cc/GET "/admin/function/:fid" [fid]
            (html-response "function.html" function comp fid))))

(defrecord AdminComponent
  [core flock-db flockreplica-db flocklog-db]
  component/Lifecycle
  (start [this]
    (->> (make-routes this)
         (assoc this :routes)))

  (stop [this]
    this)

  WebService
  (get-routes [this]
    (:routes this)))

(defn new-admin-comp
  []
  (map->AdminComponent {}))
