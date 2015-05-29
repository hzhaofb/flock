(ns flock.tasklog-test
  (:require [midje.sweet :refer :all]
            [clojure.java.jdbc :as jdbc]
            [flock.test-system :refer :all]
            [base.util :refer :all]
            [flock.worker :refer :all]
            [flock.task :refer :all]
            [flock.tasklog :refer :all]
            [flock.func :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "task-log test config log true"
       (fact
         (set-test-config! log-true)
         (start-worker (worker-comp) "1.1.1.1" 1 "java")
         (create-func (func-comp) test-func1)
         (create-task (task-comp) test-task1)
         (-> (reserve-task (task-comp) 1)
             (to-expected))
         => (assoc result-task1 :wid 1)

         (let [log1 (first (:logs (list-tasklog (tasklog-comp) 1)))]
           (select-keys log1 [:wid :event_type :error])
           => {:wid 1 :event_type "S" :error nil}
           (log1 :eta)  => (test-task1 :eta))
         (list-tasklog (tasklog-comp) 2) => {:logs ()}
         (Thread/sleep 1000)
         (set-test-config! log-true)
         (complete-task (task-comp) {:wid 1 :tid 1  :eta 123 :new_eta 3456})
         (-> (list-tasklog (tasklog-comp) 1)
             (:logs)(first) (dissoc :log_time))
         => {:tid 1 :wid 1 :event_type "C" :error nil :eta 123 :start_time nil :new_eta 3456}))


(facts "task-log test config log false"
       (fact
         (set-test-config! log-false)
         (start-worker (worker-comp) "1.1.1.1" 1 "java")
         (create-func (func-comp) test-func1)
         (create-task (task-comp) test-task1)
         (reserve-task (task-comp) 1)
         (list-tasklog (tasklog-comp) 1) => {:logs ()}
         (Thread/sleep 1000)
         (complete-task (task-comp) {:wid 1 :tid 1  :eta 123 :new_eta 3456})
         (list-tasklog (tasklog-comp) 1) => {:logs ()}))
