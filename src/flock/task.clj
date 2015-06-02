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
            [component.webservice :refer [WebService]]
            [com.stuartsierra.component :as component]
            [pallet.thread-expr :refer :all]
            [ring.middleware.json :refer [wrap-json-response]]
            [flock.func :as func]
            [flock.util :refer [mydb get-config-int]]
            [flock.tasklog :refer [write-tasklog]]
            [flock.worker :refer [get-worker-by-id]]
            [flock.server :refer [get-slot-info]]
            [base.util :refer :all]
            [base.rest-util :refer [json-response echo]]
            [compojure.core :as cc]
            [metrics.timers :refer [timer time!]]
            [metrics.histograms :refer [histogram update!]]
            [metrics.meters :refer [meter mark!]]
            [metrics.gauges :refer [gauge-fn]]
            [flock.metrics :as metrics]
            [base.util :as util])
  (:import (com.mysql.jdbc.exceptions.jdbc4 MySQLIntegrityConstraintViolationException)
           (clojure.lang PersistentQueue)))

(defn from-db
  "return map with params converted from str to map"
  [task]
  (if (some? task)
    (-> task
        (try-update :params json/parse-string)
        (when-> (nil? (task :task_key))
                (assoc :task_key (task :short_key))))))

(defn- list-tasks
  [db where args]
  (let [select "select eid, eta, t.tid, wid, fid, task_key,
                     params, short_key, reverse_domain,
                     unix_timestamp(modified) as modified
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
     (assert-pos {:fid fid})
     (assert (some? task_key) "invalid task_key")
     (->> (get-short-key task_key)
          (select-task-and-schedule (mydb comp) "t.fid=? and t.short_key=?" fid )))))

(defn- insert-task
  "insert into task table and populate the returned task with tid.
  If duplicate fid short_key, add msg \"task already exists.\" "
  [db task]
  (let [setters (select-keys task [:fid :task_key :short_key :reverse_domain :params])
        fid (:fid task)
        short_key (:short_key task)]
    (try
      (->> (jdbc/insert! db :task setters)
           (first)
           (:generated_key)
           (assoc task :tid))
      (catch MySQLIntegrityConstraintViolationException ex
        ; We have unique constraint on fid and short_key
        ; Get the tid with fid and short_key
        (jdbc/update! db :task setters ["fid=? and short_key=?" fid short_key])
        (->> ["select tid, fid, task_key, short_key, reverse_domain,
               unix_timestamp(modified) as modified, params
               from task where fid=? and short_key=?" fid short_key]
             (jdbc/query db)
             (first)
             (from-db)
             (merge task {:msg "task already exists."}))))))

(defn- insert-schedule
  "insert the schedule part of the task and returns the task itself"
  [db task]
  (try
    (jdbc/insert! db :schedule (select-keys task [:tid :eta :eid]))
    (catch MySQLIntegrityConstraintViolationException ex
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
            "eta more than 10 year in the future. Note: eta should be in epoch time." )))

(defn- to-db-key
  "process task-key related field including short_key and reverse-domain"
  [task]
  (let [task_key (:task_key task)
        short_key (get-short-key task_key)
        task (assoc task :reverse_domain (get-reverse-domain task_key))]
    (if (= short_key task_key)
      ; remove task_key to save space
      (rename-keys task {:task_key :short_key})
      (assoc task :short_key short_key))))

(defn- process-func
  "process function related fields in task before insert"
  [comp task]
  (let [fcomp (get comp :func-comp)
        f (func/get-func fcomp (task :fid))
        _ (assert (some? f) "invalid fid")]
    (merge (select-keys f [:eid]) task)))

(defn create-task
  "create a task schedule and task detail
  {:fid :task_key :eta :params}.
   Return task with tid populated.
   :params in map format is converted to json string before saved
   therefore preseves the map format when selected."
  [comp task]
  (validate-eta (task :eta))
  (assert-some task [:task_key :fid])
  (let [task (->> task
                  (process-func comp)
                  (to-db-key)
                  (to-db-params))
        db (mydb comp)]
    (->> task
         (insert-task db)
         (insert-schedule db)
         (log-time-info #(str "created task: " %) )
         (from-db))))

(defn- create-or-update-schedule
  [comp tid eta]
  (let [db (mydb comp)
        fcomp (get comp :func-comp)]
    (if (= '(0) (jdbc/update! db :schedule {:eta eta} ["tid=?" tid]))
      ; schedule is missing for this task, recreate it based on task info
      ; Since we are avoiding using transaction, it is possible when task
      ; was created but schedule was not or deleted. Following recovers schedule.
      (let [{fid :fid reverse_domain :reverse_domain}
           (->> ["select fid, reverse_domain from task where tid=?" tid]
                (jdbc/query db)
                (first))
           {eid :eid} (func/get-func fcomp fid)]
       (insert-schedule db {:tid tid :eta eta :eid eid})))))

(defn update-task
  "Update the task and return the updated task.
   If eta is specified, update the schedule.
   If either task_key or params are specified, update the task table."
  [comp tid task_key eta params]
  (let [tid (to-int tid "tid")
        _ (assert (> tid 0))]
    (if (some? eta)
      ; if specified eta, update the schedule
      (let [eta (to-int eta)]
        (assert (> eta 0))
        (create-or-update-schedule comp tid (to-int eta))))
    (let [keys (if (some? task_key)
                   (to-db-key {:task_key task_key}))
          update (if (some? params)
                   (assoc keys :params (json/generate-string params))
                   keys)]
      (if update
        (jdbc/update! (mydb comp) :task update ["tid=?" tid])))
    (->> (get-task-by-id comp tid)
         (log-time-info #(str "updated task " %)))))

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
          ; set the pull_time here before query to avoid duplicate pulls
          _ (swap! task-cache assoc eid {:pull_time (System/currentTimeMillis)})
          batch-size (get-config-int comp "flock.task.cache.size" 50)
          sql "select tid, eta from schedule where eta < unix_timestamp(now())
               and wid = 0 and eid = ?
               order by eta ASC limit ?"
          query-result (->> (jdbc/query (mydb comp) [sql eid batch-size])
                            (log-time-info #(str "filling task cache got " (count %))))]
      (some->> (not-empty query-result)
               (apply conj (PersistentQueue/EMPTY))
               (ref)
               (assoc {:pull_time (System/currentTimeMillis)} :qref)
               (swap! task-cache assoc eid)))))

(defn- take-candidate?
  "check if candidate should be processed by this server"
  [comp cand]
  (let [tid (:tid cand)
        {slot :slot server_count :server-count}
        (get-slot-info (:server-comp comp))]
    (->> (max server_count 1)                               ; protect against 0 length
         (rem tid)
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
        (let [task (get-task-by-id comp (:tid cand))
              tlog (assoc task :event_type "S")]
          (write-tasklog (get comp :tasklog-comp) tlog)
          (mark! metrics/reserved-meter)
          task)
        (recur (dequeue-candidate-cache comp eid))))))

(defn reserve-task
  "reserves a task for a given worker and return map with positive tid and eta.
   After worker complete the task, it must call complete-task with new with :new_eta.
   return nil if no task is due."
  [comp wid]
  (time!
    metrics/reserve-db-timer
    (let [wid (to-int wid "wid")
          worker-comp (get comp :worker-comp)
          worker (get-worker-by-id worker-comp wid)
          _ (assert (some? worker) (str "worker " wid " not registered") )
          eid (worker :eid)]
      ; can't use some->> because it evaluate the reserve-task
      ; and log-time-info will not give the right timing
      (->> (reserve-task-impl comp wid eid)
           (log-time-info #(str "wid=" wid " got tid=" (:tid %)))))))

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
    (metrics/update-worker-slack! wid (- limit tcount))
    tasks))

(defn- complete-task-db
  "returns keyword
    :rescheduled when valid new_eta is specified
    :deleted when no new_eta
    :not-reserved when wid didn't reserve tid"
  [db tid wid new_eta]
  (if (and (some? new_eta) (pos? new_eta))
    ;reschedule task to new_eta and indicate if successful
    (if (= '(0)
           (jdbc/update! db :schedule {:wid 0 :eta new_eta}
                         ["tid=? and wid=?" tid wid]))
      :not-reserved
      :rescheduled)
    ; no new eta, delete schedule and task
    (if (= '(0) (jdbc/delete! db :schedule ["tid=? and wid=?" tid wid]))
      :not-reserved
      :deleted)))

(defn- complete-result-to-msg
  [result tid wid new_eta]
  (case result
    :rescheduled (str "rescheduled task tid=" tid " new_eta=" new_eta)
    :deleted (str "no new_eta, task tid=" tid " deleted")
    :not-reserved (str "task tid=" tid " is not reserved by worker wid=" wid)))

(defn- complete-task-impl
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
        start_time (:start_time task-report)
        start_time (if (some? start_time)
                     (to-int start_time "start_time"))
        db (mydb comp)
        log-comp (get comp :tasklog-comp)
        error (:error task-report)
        tlog {:wid wid :tid tid
              :eta eta :new_eta new_eta
              :start_time start_time
              :error error :event_type "C"}]
    (if error (mark! metrics/error-meter))
    (when start_time
      (update! metrics/queuing-time (- start_time eta))
      (update! metrics/processing-time (- (util/current-epoch) start_time)))
    (let [result
          (->> (complete-task-db db tid wid new_eta)
               (log-time-info #(str "wid=" wid " completes tid=" tid " " %)))
          msg (complete-result-to-msg result tid wid new_eta)]
      (if (= :not-reserved result)
        (throw (Exception. msg))
        (do (write-tasklog log-comp tlog)
            {:msg msg})))))

(defn complete-task
  "report task complete. task-report should include
  {:wid wid :tid tid :eta eta :new_eta new_eta (optional) :error (if failed)
  If :new_eta is not nil, reschedule the task"
  [comp task-report]
  (time!
    metrics/complete-db-timer
    (try
      (let [result (complete-task-impl comp task-report)]
        (mark! metrics/completed-meter)
        result)
      (catch Exception ex
        (mark! metrics/error-meter)
        (throw ex)))))

(defn delete-task
  "delete the task with tid.
  If the task is being executed, delete the schedule
  so it will not be rescheduled."
  [comp tid]
  (log/info "delete tid=" tid)
  (let [[count]  (jdbc/delete! (mydb comp) :schedule ["tid=?" tid])]
    (jdbc/delete! (mydb comp) :task ["tid=?" tid])
    (if (= 1 count)
      {:msg "task deleted"}
      {:msg (str "task " tid " doesn't exist") })))

(defn list-func-backlog
  "returns a list of tasks for this function with eta < now"
  [comp fid]
  (let [fcomp (get comp :func-comp)
        f (func/get-func fcomp fid)
        eid (:eid f)]
    ; Can be expensive. MySql should scan schedule index
    ;  wid, eid, eta
    (if eid
      (list-tasks (mydb comp)
                  "s.wid = 0 and s.eta < unix_timestamp(now())
                   and s.eid = ? and t.fid = ?"
                  [eid fid]))))

(defn count-func-backlog
  "returns the count of tasks for this function with eta < now"
  [comp fid]
  (let [fcomp (get comp :func-comp)
        fid (to-int fid "fid")
        f (func/get-func fcomp fid)
        eid (:eid f)]
    (-> (if eid
          (-> (jdbc/query
                (mydb comp)
                ["select count(*) as backlog_count
                   from schedule s
                   join task t on t.tid = s.tid
                   where s.wid = 0 and s.eta < unix_timestamp(now())
                   and s.eid = ? and t.fid = ?"
                 eid fid])
              (first))
          {:error "unknown fid"})
        (assoc :fid fid))))

(defn- make-routes
  [comp]
  (cc/routes
    (cc/GET "/worker/:wid/task" [wid :as req]
            ;; reserves a task for a given worker wid.
            ;; worker must PUT back with new_eta after complete
            (json-response req reserve-task comp wid))
    (cc/GET "/worker/:wid/tasks" [wid limit :as req]
            ;; reserves tasks upto the limit for a given worker wid.
            ;; worker must PUT back each task with new_eta after complete
            (json-response req reserve-tasks comp wid limit))
    (cc/PUT "/worker/:wid/task/:tid" [wid tid new_eta error :as req]
            ;; report task complete with new eta.
            (json-response req complete-task comp (req :params)))
    (cc/PUT "/task/cache" [:as req]
            ;; clear task candidate list cache
            (json-response req reset-task-cache comp))
    (cc/GET "/task/:tid" [tid :as req]
            ;; get task with id tid
            (json-response req get-task-by-id comp tid))
    (cc/DELETE "/task/:tid" [tid :as req]
            ;; delete task with id tid
            (json-response req delete-task comp tid))
    (cc/GET "/task" [fid task_key :as req]
            ;; get task with given fid and task_key as query params
            (json-response req get-task-by-fid-key comp fid task_key))
    (cc/POST "/task" req
             ;; create a new task
             (json-response req create-task comp (req :params)))
    (cc/PUT "/task/:tid" [tid task_key eta params :as req]
            ;; update existing task
            (json-response req update-task comp tid task_key eta params))
    (cc/GET "/func/:fid/backlog" [fid :as req]
            ;; get function with id
            (json-response req list-func-backlog comp fid))
    (cc/GET "/func/:fid/backlog_count" [fid :as req]
            ;; get function with id
            (json-response req count-func-backlog comp fid))

    (cc/POST "/ptask" req
             ;; post on this endpoint for sanity test
             (json-response req echo req))
    ))

(defrecord TaskComponent
           [core flock-db worker-comp
            func-comp tasklog-comp resource-comp]
  component/Lifecycle
  (start [this]
    (let [comp (assoc this :task-cache (atom {}))
          routes (->> (make-routes comp)
                      (wrap-json-response))]
      (assoc comp :routes routes)))

  (stop [this]
    this)

  WebService
  (get-routes [this]
    (:routes this)))

(defn new-task-comp
  []
  (map->TaskComponent {}))
