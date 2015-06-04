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