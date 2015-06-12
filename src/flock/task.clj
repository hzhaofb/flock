;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; flock.task defines an instance of task that worker can execute
;; properties: tid, fid (func-id), task-key, eta, params
;; - fid and task-key should be unique
;; - For performance and mysql constraint limit, if task_key is longer than 40, store sha1 in
;;   short_key. Otherwise only store task_key in short key.
;; - For domain level management, task-key in URL is used to populate the reverse-domain column
;; - For performance of task reservation, task is stored in two tables task, schedule
;; - the wid (worker id) column of > 0 indicates that the worker is working on the task

(ns flock.task
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [rename-keys]]
            [clj-time.core :as tc]
            [clj-time.coerce :as tcc]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [flock.func :as func]
            [component.database :refer [mydb]]
            [component.core :refer [get-config-int]]
            [flock.worker :refer [get-worker-by-id]]
            [flock.server :refer [get-slot-info]]
            [base.util :refer :all]
            [base.rest-util :refer [json-response echo]])
  (:import (clojure.lang PersistentQueue)
           (java.sql SQLException)))

(defn from-db
  "return map with params converted from str to map"
  [task]
  (let [task (try-update task :params json/parse-string)]
    (if (nil? (:task_key task))
      (some-> task (assoc :task_key (:short_key task)))
      task)))

(defn- list-tasks
  [db where args]
  (let [select "select eid, eta, t.tid, wid, fid, task_key,
                     params, short_key, unix_timestamp(modified) as modified
                from task t join schedule s on s.tid = t.tid where "
        sql (str select where)
        q (into [sql] args)]
    (->> (jdbc/query db q)
         (map from-db))))

(defn- get-short-key
  [task_key]
  (if (> (count task_key) 40)
    (get-sha1-hash task_key)
    task_key))

(defn- select-task-and-schedule
  [db where & args]
  (-> (list-tasks db where args)
      (first)))

(defn get-task-by-id
  "return a task for given tid"
  ([comp tid]
   (let [tid (to-int tid "tid")]
     (assert-pos {:tid tid})
     (select-task-and-schedule (mydb comp) "t.tid=?" tid))))

(defn get-task-by-fid-key
  "return a task for given fid and task_key"
  ([comp fid task_key]
   (let [fid (to-int fid "fid")]
     (assert (pos? fid))
     (assert (some? task_key) "invalid task_key")
     (->> (get-short-key task_key)
          (select-task-and-schedule (mydb comp) "t.fid=? and t.short_key=?" fid )))))

(defn- insert-task
  "insert into task table and populate the returned task with tid.
  If duplicate fid short_key, add msg \"task already exists.\" "
  [db task]
  (let [setters (select-keys task [:fid :task_key :short_key :params])]
    (try
      (->> (jdbc/insert! db :task setters)
           (first)
           (:generated_key)
           (assoc task :tid))
      (catch SQLException ex
        ; We have unique constraint on fid and short_key
        ; Get the tid with fid and short_key
        (let [{fid :fid short_key :short_key} task]
          (jdbc/update! db :task setters ["fid=? and short_key=?" fid short_key])
          (->> ["select tid from task where fid=? and short_key=?" fid short_key]
               (jdbc/query db)
               (first)
               (merge task {:msg "task already exists."})))))))

(defn- insert-schedule
  "insert the schedule part of the task and returns the task itself"
  [db task]
  (try
    (jdbc/insert! db :schedule (select-keys task [:tid :eta :eid]))
    (catch SQLException ex
      (jdbc/update! db :schedule
                    (select-keys task [:eta])
                    ["tid=?" (task :tid)])))
  (assoc task :wid 0))

(defn- to-db-params
  [task]
  (update-in task [:params] json/generate-string))

(defn- validate-eta [eta]
  (assert (some? eta) "eta must be provided")
  (assert (pos? eta) "eta need to be positive epoch time")
  ; check to make sure eta is less than 10 year in the future
  (let [ten-year (tc/plus (tc/now) (tc/years 10))]
    (assert (< eta (tcc/to-epoch ten-year))
            "eta more than 10 year in the future. Note: eta should be in epoch time." ))
  true)

(defn- to-db-key
  "process task-key related field including short_key and reverse-domain"
  [task]
  (->> (:task_key task)
       (get-short-key)
       (assoc task :short_key)))

(defn- process-func
  "process function related fields in task before insert"
  [comp task]
  (let [fcomp (get comp :func-comp)
        f (func/get-func fcomp (task :fid))]
    (assert (some? f) "invalid fid")
    (merge (select-keys f [:eid]) task)))

(defn create-task
  "create a task schedule and task detail
  {:fid :task_key :eta :params}.
   Return task with tid populated.
   :params in map format is converted to json string before saved
   therefore preseves the map format when selected."
  [comp task]
  (validate-eta (:eta task))
  (assert-some task [:task_key :fid])
  (->> task
       (process-func comp)
       (to-db-key)
       (to-db-params)
       (insert-task (mydb comp))
       (insert-schedule (mydb comp))
       (log-time-info #(str "created task: " %) )
       (from-db)))

(defn- create-or-update-schedule
  [db tid eta]
  (if (= '(0) (jdbc/update! db :schedule {:eta eta} ["tid=?" tid]))
    ; schedule is missing for this task, recreate it based on task info
    (jdbc/execute! db ["insert into schedule (tid, eta, eid)
                            select t.tid, ?, f.eid
                            from task as t join func as f
                            on t.fid = f.fid
                            where t.tid = ?" eta tid])))

(defn- update-task-impl
  "Update the task and return the updated task.
   If eta is specified, update the schedule.
   If either task_key or params are specified, update the task table."
  [comp tid task_key eta params]
  (let [tid (to-int tid "tid")
        _ (assert (> tid 0))
        db (mydb comp)]
    (when (some? eta)
      ; if specified eta, update the schedule
      (let [eta (to-int eta)]
        (validate-eta eta)
        (create-or-update-schedule db tid eta)))
    (let [update (when (some? task_key)
                   (to-db-key {:task_key task_key}))
          update (if (some? params)
                   (assoc update :params (json/generate-string params))
                   update)]
      (if update
        (jdbc/update! db :task update ["tid=?" tid])))
    (get-task-by-id comp tid)))

(defn update-task
  [comp tid task_key eta params]
  (->> (update-task-impl comp tid task_key eta params)
       (log-time-info #(str "updated task " %))))

(defn- need-refresh-cache?
  [comp eid]
  (let [{qref :qref pull_time :pull_time}
        (some-> (get comp :task-cache)
                (deref)
                (get eid))
        min-pull
        (get-config-int comp "flock.task.cache.min.pull.interval.secs" 5)]
    (cond
      ; never pulled before
      (nil? pull_time)
      true
      ; passed min pull time, refresh if empty
      (> (System/currentTimeMillis) (+ pull_time (* min-pull 1000)))
      (or (nil? qref) (empty? @qref))
      :else
      ; not passed min delay time yet
      false)))

(defn- ensure-task-cache
  "ensure tid cache is populated. Load if it's nil or empty.
  task cache is an atom of map {eid {:qref (ref PersistentQueue({:tid 1 :eta 12345} ....))
                                     :pull_time last pull time}"
  [comp eid]
  ; since we are filter based on server id, we can have empty queue only pickup
  ; tasks intended for other servers repeatedly in infinite loop.
  ; must prevent this from happening by checking timestamp for last pull
  (when (need-refresh-cache? comp eid)
    (let [task-cache (get comp :task-cache)
          batch-size (get-config-int comp "flock.task.cache.size" 50)]
      ; set the pull_time here before query to avoid duplicate pulls
      (swap! task-cache assoc eid {:pull_time (System/currentTimeMillis)})
      (some->> ["select tid, eta from schedule
                 where eta < unix_timestamp(now())
                   and wid = 0 and eid = ?
                 order by eta ASC limit ?" eid batch-size]
               (jdbc/query (mydb comp))
               (log-time-info #(str "filling task cache got " (count %)))
               (not-empty)
               (apply conj (PersistentQueue/EMPTY))
               (ref)
               (assoc {:pull_time (System/currentTimeMillis)} :qref)
               (swap! task-cache assoc eid)))))

(defn- take-candidate?
  "check if candidate should be processed by this server"
  [comp cand]
  (let [{slot :slot server_count :server-count}
        (get-slot-info (:server-comp comp))]
    ; protect against 0 length
    (->> (max server_count 1)
         (rem (:tid cand))
         (= slot))))

(defn- dequeue-candidate-cache
  "take the next task from cache for a given eid"
  [comp eid]
  (some-> (get comp :task-cache)
          (deref)
          (get-in [eid :qref])
          (dequeue!)))

(defn reset-task-cache
  "reset the candidate cache"
  [comp]
  (log/info "reset task cache")
  (reset! (get comp :task-cache) {})
  {:msg "task cache refreshed"})

(defn- reserve-task-db
  "reserve task in the database by updating the schedule table.
   Return 1 if successfully reserved, 0 otherwise"
  [comp wid {tid :tid eta :eta}]
  (-> (jdbc/update! (mydb comp) :schedule {:wid wid}
                    ["wid = 0 and tid=? and eta=?" tid eta])
      (first)))

(defn- reserve-task-impl
  "see reserved-task using task-cache. If we have a task candidate, use it
  to update the wid given the record is not changed.
  If no row updated, try other candidates."
  [comp wid eid]
  (ensure-task-cache comp eid)
  (loop [cand (dequeue-candidate-cache comp eid)]
    (when (some? cand)
      (if (and (take-candidate? comp cand)
               (= 1 (reserve-task-db comp wid cand)))
        ; get the entire task by id
        (get-task-by-id comp (:tid cand))
        (recur (dequeue-candidate-cache comp eid))))))

(defn reserve-task
  "reserves a task for a given worker and return map with positive tid and eta.
   After worker complete the task, it must call complete-task with new with :new_eta.
   return nil if no task is due."
  [comp wid]
  (let [wid (to-int wid "wid")
        worker-comp (get comp :worker-comp)
        worker (get-worker-by-id worker-comp wid)
        _ (assert (some? worker) (str "worker " wid " not registered") )
        eid (worker :eid)]
    ; can't use some->> because it evaluate the reserve-task
    ; and log-time-info will not give the right timing
    (->> (reserve-task-impl comp wid eid)
         (log-time-info #(str "wid=" wid " got tid=" (:tid %))))))

(defn reserve-tasks
  "reserve tasks for a given worker upto the limit."
  [comp wid limit]
  (let [limit (to-int limit)
        _ (assert (> limit 0))
        tasks (->> #(reserve-task comp wid)
                   (repeatedly)
                   (take-while some?)
                   (take limit))
        tcount (count tasks)]
    (log/info "wid" wid "asked" limit "got" tcount)
    tasks))

(defn- complete-task-db
  "returns keyword
    :rescheduled when valid new_eta is specified
    :deleted when no new_eta
    :not-reserved when wid didn't reserve tid"
  [db tid wid new_eta]
  (let [where ["tid=? and wid=?" tid wid]]
    (if (and (some? new_eta) (validate-eta new_eta))
     ;reschedule task to new_eta and indicate if successful
     (if (= '(0) (jdbc/update! db :schedule {:wid 0 :eta new_eta} where))
       :not-reserved
       :rescheduled)
     ; no new eta, delete schedule and task
     (if (= '(0) (jdbc/delete! db :schedule where))
       :not-reserved
       :deleted))))

(defn- complete-result-to-msg
  [result tid wid new_eta]
  (case result
    :rescheduled (str "rescheduled task tid=" tid " new_eta=" new_eta)
    :deleted (str "no new_eta, task tid=" tid " deleted")
    :not-reserved (str "task tid=" tid " is not reserved by worker wid=" wid)))

(defn complete-task
  "report task complete. task-report should include
   {:wid wid :tid tid :eta eta :new_eta new_eta (optional) :error (if failed)
   If :new_eta is not nil, reschedule the task"
  [comp task-report]
  (let [wid (to-int (:wid task-report) "wid")
        _ (assert (pos? wid) "invalid wid")
        tid (to-int (:tid task-report) "tid")
        _ (assert (pos? tid) "invalid tid")
        eta (:eta task-report)
        eta (if (some? eta)
              (to-int eta))
        _ (assert (pos? eta) "invalid eta")
        new_eta (:new_eta task-report)
        new_eta (if (some? new_eta)
                  (to-int new_eta "new_eta"))
        db (mydb comp)]
    (let [result
          (->> (complete-task-db db tid wid new_eta)
               (log-time-info #(str "wid=" wid " completes tid=" tid " " %)))
          msg (complete-result-to-msg result tid wid new_eta)]
      (if (= :not-reserved result)
        (throw (Exception. msg))
        {:msg msg}))))

(defn delete-task
  "delete the task with tid.
  If the task is being executed, delete the schedule
  so it will not be rescheduled."
  [comp tid]
  (log/info "delete task tid=" tid)
  (let [[count]  (jdbc/delete! (mydb comp) :schedule ["tid=?" tid])]
    (jdbc/delete! (mydb comp) :task ["tid=?" tid])
    (if (= 1 count)
      {:msg "task deleted"}
      {:msg (str "task " tid " doesn't exist") })))

(defrecord TaskComponent
           [core flock-db worker-comp]
  component/Lifecycle
  (start [this]
    (assoc this :task-cache (atom {})))

  (stop [this]
    this))

(defn new-task-comp
  []
  (map->TaskComponent {}))
