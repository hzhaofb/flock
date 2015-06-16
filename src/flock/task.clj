;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; flock.task defines an instance of task that worker can execute
;; properties: tid, fid (func-id), task-key, eta, params
;; - fid and task-key should be unique
;; - For performance and mysql constraint limit, if task_key is longer than 40, store sha1 in
;;   short_key. Otherwise only store task_key in short key.
;; - For performance of task reservation, task is stored in two tables task, schedule
;; - the wid (worker id) column of > 0 indicates that the worker is working on the task

(ns flock.task
  (:require [clojure.java.jdbc :as jdbc]
            [clj-time.core :as tc]
            [clj-time.coerce :as tcc]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [flock.func :as func]
            [component.database :refer [mydb get-single-row insert-row]]
            [component.core :refer [get-config-int]]
            [flock.worker :refer [get-worker-by-id]]
            [flock.server :refer [take-candidate?]]
            [base.util :refer :all])
  (:import (clojure.lang PersistentQueue)))

(defn- get-short-key
  [task_key]
  (if (> (count task_key) 40)
    (get-sha1-hash task_key)
    task_key))

(defn from-db
  "return map with params converted from str to map"
  [task]
  (try-update task :params json/parse-string))

(defn- list-tasks
  [db where args]
  (-> "select eid, eta, t.tid, wid, fid, task_key,
              params, short_key, unix_timestamp(modified) as modified
        from task t join schedule s on s.tid = t.tid where "
      (str where)
      (cons args)
      (->> (jdbc/query db)
           (map from-db))))

(defn- select-task-and-schedule
  [db where & args]
  (-> (list-tasks db where args)
      (first)))

(defn get-task-by-id
  "return a task for given tid"
  ([comp tid]
   (select-task-and-schedule (mydb comp)
     "t.tid=?" (to-int tid "tid"))))

(defn get-task-by-fid-key
  "return a task for given fid and task_key"
  ([comp fid task_key]
   (let [fid (to-int fid "fid")
         short_key (get-short-key task_key)]
     (assert (pos? fid))
     (assert (some? task_key) "invalid task_key")
     (select-task-and-schedule (mydb comp) "t.fid=? and t.short_key=?" fid short_key))))

(defn- upsert-task
  "insert into task table and populate the returned task with tid.
  If duplicate fid short_key, add msg \"task already exists.\" "
  [comp task]
  (let [db (mydb comp)
        {:keys [fid short_key]} task
        setters (select-keys task [:fid :task_key :short_key :params])]
    (if (= '(1) (jdbc/update! db :task setters
                              ["fid=? and short_key=?" fid short_key]))
      ; cannot select schedule portion since it may not exist
      (->> (jdbc/query db ["select tid from task where fid=? and short_key=?" fid short_key])
           (first)
           (merge task {:msg "task already exists."}))
      (->> (insert-row db :task setters)
           (assoc task :tid)))))

(defn- upsert-schedule
  "insert the schedule part of the task and returns the task itself"
  [db {:keys [eta tid eid] :as task}]
  (if (= '(0) (jdbc/update! db :schedule {:eta eta} ["tid=?" tid]))
    (jdbc/insert! db :schedule {:tid tid :eta eta :eid eid}))
  (assoc task :wid 0))

(defn- validate-eta [eta]
  (when (some? eta)
    (let [eta (to-int eta)
          ten-year (tc/plus (tc/now) (tc/years 10))]
      (assert (pos? eta) "eta need to be positive epoch time")
      (assert (< eta (tcc/to-epoch ten-year))
              "eta more than 10 year in the future. Note: eta should be in epoch time." )
      eta)))

(defn- process-func
  "process function related fields in task before insert"
  [comp task]
  (let [fcomp (get comp :func-comp)
        {eid :eid} (func/get-func fcomp (task :fid))]
    (assert (some? eid) "invalid task fid")
    (assoc task :eid eid)))

(defn create-task
  "create a task schedule and task detail
  {:fid :task_key :eta :params}.
   Return task with tid populated.
   :params in map format is converted to json string before saved
   therefore preseves the map format when selected."
  [comp {:keys [eta params task_key] :as task}]
  (assert (some? eta) "eta must be provided")
  (validate-eta eta)
  (assert-some task [:task_key :fid])
  (->> {:params (json/generate-string params)}
       (merge {:short_key (get-short-key task_key)})
       (merge task)
       (process-func comp)
       (upsert-task comp)
       (upsert-schedule (mydb comp))
       (log-time-info #(str "created task: " %) )
       (from-db)))

(defn- update-task-impl
  "Update the task and return the updated task.
   If eta is specified, update the schedule.
   If either task_key or params are specified, update the task table."
  [comp tid task_key eta params]
  (let [tid (to-int tid "tid")
        db (mydb comp)]
    (when (some? eta)
      (let [{fid :fid} (-> (jdbc/query db ["select fid from task where tid=?" tid])
                           (first))
            {eid :eid} (func/get-func (:func-comp comp) fid)]
        (validate-eta eta)
        (assert (some? eid) (str "task tid=" tid " doesn't exist"))
        (upsert-schedule db {:tid tid :eta eta :eid eid})))
    (let [update (when (some? task_key)
                   {:task_key task_key :short_key (get-short-key task_key)})
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

; task-cache is atom of map
; {eid {:qref <ref to task queue> :pull_time <last pulled time>}}
; initially task-cache is empty
(defn- refresh-cache?
  [comp eid]
  (let [{:keys [qref pull_time]}
        (some-> (get comp :task-cache)
                (deref)
                (get eid))
        min-pull (get-config-int comp "flock.task.cache.min.pull.interval.secs" 5)]
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
  "ensure tid cache is populated. Load if it's nil or empty."
  [comp eid]
  ; since we are filter based on server id, we can have empty queue only pickup
  ; tasks intended for other servers repeatedly in infinite loop.
  ; must prevent this from happening by checking timestamp for last pull
  (when (refresh-cache? comp eid)
    (let [task-cache (:task-cache comp)
          batch-size (get-config-int comp "flock.task.cache.size" 50)]
      ; set the pull_time here before query to avoid duplicate queries
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

(defn- reserve-task-impl
  "see reserved-task using task-cache. If we have a task candidate, use it
  to update the wid given the record is not changed.
  If no row updated, try other candidates."
  [comp wid eid]
  (ensure-task-cache comp eid)
  (loop [{:keys [tid eta] :as cand}
         (dequeue-candidate-cache comp eid)]
    (when cand
      (if (and (take-candidate? (:server-comp comp) tid)
               (= '(1) (jdbc/update! (mydb comp) :schedule {:wid wid}
                                     ["wid = 0 and tid=? and eta=?" tid eta])))
        ; returns the full task
        (get-task-by-id comp tid)
        (recur (dequeue-candidate-cache comp eid))))))

(defn- validate-worker-eid
  "returns eid of the worker and assert it's valid"
  [comp wid]
  (let [{:keys [eid]} (get-worker-by-id (:worker-comp comp) wid)]
    (assert (some? eid) (str "worker " wid " not registered"))
    eid))

(defn reserve-task
  "reserves a task for a given worker and return map with positive tid and eta.
   After worker complete the task, it must call complete-task with new with :new_eta.
   return nil if no task is due."
  ([comp wid eid]
   (->> (reserve-task-impl comp wid eid)
        (log-time-info #(str "wid=" wid " got tid=" (:tid %)))))
  ([comp wid]
   (->> (validate-worker-eid comp wid)
        (reserve-task comp wid))))

(defn reserve-tasks
  "reserve tasks for a given worker upto the limit."
  [comp wid limit]
  (let [limit (to-int limit)
        _ (assert (pos? limit) "limit must be positive")
        eid (validate-worker-eid comp wid)
        tasks (->> #(reserve-task comp wid eid)
                   (repeatedly)
                   (take-while some?)
                   (take limit))
        tcount (count tasks)]
    (log/info "wid" wid "asked" limit "got" tcount)
    tasks))

(defn complete-task
  "report task complete. task-report should include
   {:wid wid :tid tid :eta eta :new_eta new_eta (optional) :error (if failed)
   If :new_eta is not nil, reschedule the task"
  [comp {:keys [wid tid new_eta]}]
  (let [wid (to-int wid "wid")
        tid (to-int tid "tid")
        new_eta (validate-eta new_eta)
        not-reserved (str "task tid=" tid " is not reserved by worker wid=" wid)
        where ["tid=? and wid=?" tid wid]
        db (mydb comp)]
    (if (some? new_eta)
      (if (= '(0) (jdbc/update! db :schedule {:wid 0 :eta new_eta} where))
        (throw (Exception. not-reserved))
        {:msg (str "rescheduled task tid=" tid " new_eta=" new_eta)})
      (if (= '(0) (jdbc/delete! db :schedule where))
        (throw (Exception. not-reserved))
        {:msg (str "no new_eta, task tid=" tid " deleted")}))))

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
