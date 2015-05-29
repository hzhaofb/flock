(ns flock.task-test-int
  (:require [midje.sweet :refer :all]
            [flock.test-system :refer :all]
            [base.util :as fu]
            [base.rest-util :refer :all]
            [component.core :refer [set-config!]])
  (:import (java.net URLEncoder)))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (do
                    (setup-fact)
                    (rest-put url "task" "cache" {})
                    (rest-post url "func" test-func1)))
   (after :facts (teardown-test))])


(def log-keys [:tid :wid :eta :event_type :error :start_time :new_eta])

(defn to-rest-expected
  [t]
  (-> (assoc t :params (keyify (:params t)))
      (dissoc :maxpar)))

(def expected-task1 (to-rest-expected test-task1))

(def expected-task2 (to-rest-expected test-task2))

(facts "task management REST API integration test"
       (fact
         ; create task
         (-> (rest-post url "task" test-task1)
             (select-keys (keys test-task1)))
         => expected-task1

         ; get the task by id
         (-> (rest-get url "task" 1)
             (select-keys (keys test-task1)))
         => expected-task1

         ; get task by func and key
         (let [encoded (URLEncoder/encode (test-task1 :task_key))]
           (-> (rest-get (str url "/task?fid=1&task_key=" encoded))
               (select-keys (keys test-task1))))
         => expected-task1

         ; update only eta
         (rest-put url "task" 1 {:eta 1222})
         (-> (rest-get url "task" 1)
             (select-keys (keys test-task1)))
         => (assoc expected-task1 :eta 1222)

         (let [update {:task_key "new task key for task 1 no eta test"
                       :params {:key1 "updated params no eta test"}}]
           ; update only update the task
           (rest-put url "task" 1 update)
           ; get the task to check update
           (-> (rest-get url "task" 1)
               (select-keys (keys test-task1)))
           => (-> (into test-task1 update)
                  (assoc :eta 1222)))

         (let [update {:eta 12345
                       :task_key "new task key for task 1 with eta"
                       :params {:key1 "updated params with eta"}}]
           ; update the task eta and setting
           (rest-put url "task" 1 update)
           ; get the task to check update
           (-> (rest-get url "task" 1)
               (select-keys (keys test-task1)))
           => (into test-task1 update))

         ;delete task tests
         (:msg (rest-delete url "task" 1)) => "task deleted"
         (rest-get url "task" 1 ) => {:status "Not Found"}
         (:msg (rest-delete url "task" 1)) => "task 1 doesn't exist" ))



(facts "task reservation and tasklog REST API integration test "
       (fact
         ; create tasks
         (rest-post url "task" test-task1)
         (rest-post url "task" test-task2)
         ; clear task cache
         (rest-put url "task" "cache" {})

         ; create worker first
         (let [{wid :wid} (rest-post url "worker" {:ip "1.1.1.1" :pid 2 :env "java"})]

           (set-test-config! log-true)
           (-> (rest-get url "worker" wid "task")
               (select-keys (keys test-task1)))
           => expected-task1

           ; reserve second task
           (set-test-config! log-true)
           (-> (rest-get url "worker" wid "task")
               (select-keys (keys test-task2)))
           => expected-task2

           ; so the log will be in order
           (Thread/sleep 1000)
           ; complete first task with new eta
           (set-test-config! log-true)
           (-> (rest-put url "worker" wid "task" 1
                         {:eta 123 :new_eta 234 :start_time 123})
               (:msg))
           => "rescheduled task tid=1 new_eta=234"

           (rest-put url "worker" 99 "task" 1
                     {:eta 123 :new_eta 234 :start_time 123})
           => {:status "error" :msg "task tid=1 is not reserved by worker wid=99"}

           ; task 1 eta should be updated
           (-> (rest-get url "task" 1)
               (select-keys (keys test-task1)))
           => (into expected-task1 {:eta 234})

           ; task log should have two records for task1
           (let [logs (-> (rest-get url "tasklog" 1)
                          (:logs))]
             (dissoc (first logs) :log_time)
             =>  {:tid 1 :wid wid :eta 123 :start_time 123 :new_eta 234
                  :event_type "C" :error nil}
             (dissoc (second logs) :log_time)
             =>  {:tid 1 :wid wid :eta 123 :start_time nil :new_eta nil
                  :event_type "S" :error nil})
           ; complete task2 with no new eta
           (-> (rest-put url "worker" wid "task" 2 {:eta 125 :start_time 124})
               (:msg))
           => "no new_eta, task tid=2 deleted"

           ; task 2 should be gone
           (rest-get url "task" 2)
           => {:status "Not Found"}

           ; task log should have two records for task2
           (let [logs (-> (rest-get url "tasklog" 2)
                          (:logs))]
             (-> (first logs)
                 (select-keys log-keys))
             => {:tid 2 :wid wid :eta 125 :event_type "C" :error nil :new_eta nil :start_time 124}

             (-> (second logs)
                 (select-keys log-keys))
             => {:tid 2 :wid wid :eta 125 :event_type "S" :error nil :new_eta nil :start_time nil})


           (Thread/sleep 1000)

           ; clear task cache
           (rest-put url "task" "cache" {})

           ; reserve first task again
           (-> (rest-get url "worker" wid "task")
               (select-keys (keys test-task1)))
           => (assoc expected-task1 :eta 234)

           (Thread/sleep 1000)
           ;; complete task1 with new eta and error
           (-> (rest-put url "worker" wid "task" 1
                         {:eta 123 :new_eta 345
                          :error "test error" :start_time 124})
               (:msg))
           => "rescheduled task tid=1 new_eta=345"

           ; task log should have two records
           (let [logs (-> (rest-get url "tasklog" 1)
                          (:logs))]
             (-> (first logs)
                 (select-keys log-keys))
             => {:tid 1 :wid wid :eta 123 :new_eta 345 :start_time 124
                 :event_type "C" :error "test error"}
             (-> (second logs)
                 (select-keys log-keys))
             => {:tid 1 :wid wid :eta 234 :event_type "S" :new_eta nil :start_time nil :error nil}))))

(facts "task reservation REST API integration test negative cases"
       (fact
         ; non exist worker

         (-> (rest-get url "worker" 1 "task")
             (:msg)) => "Assert failed: worker 1 not registered\n(some? worker)"

         ; create worker first
         (let [{wid :wid} (rest-post url "worker" {:ip "1.1.1.1" :pid 3 :env "java"})]

           (rest-get url "worker" wid "task")
           => {:status "Not Found"}

           ; create task eta in the future
           (let [t (assoc test-task1 :eta (+ 1 (fu/current-epoch)))]
             (rest-post url "task" t)
             ; try reserve task where there is none
             (rest-get url "worker" wid "task")
             => {:status "Not Found"}
             (Thread/sleep 2000)

             ; clear task cache
             (rest-put url "task" "cache" {})
             (-> (rest-get url "worker" wid "task")
                 (select-keys (keys t)))
             => (to-rest-expected t)))))

(facts "list and count func backlog task REST"
       (fact
         (rest-post url "task" test-task1)
         (rest-post url "task" test-task2)
         (rest-get url "func" 1 "backlog_count")
         => {:fid 1 :backlog_count 2 :status "OK"}
         (rest-get url "func" 99 "backlog_count")
         => {:fid 99 :error "unknown fid" :status "OK"} ))


(facts "reserve tasks test"
       (fact
         (rest-post url "task" test-task1)
         (rest-post url "task" test-task2)

         ; clear task cache
         (rest-put url "task" "cache" {})

         (let [{wid :wid} (rest-post url "worker" {:ip "1.1.1.1" :pid 2 :env "java"})
               {tasks :result} (rest-get url "worker" wid "tasks?limit=10")]

           (-> (first tasks)
               (select-keys (keys test-task1)))
           => expected-task1

           (-> (second tasks)
               (select-keys (keys test-task1)))
           => expected-task2)))