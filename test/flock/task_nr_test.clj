;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 2/25/15
;; flock.task_nr_test
;; 
;; Purpose:
;;

(ns flock.task-nr-test
  (:require [midje.sweet :refer :all]
            [flock.test-system :refer :all]
            [base.util :refer :all]
            [flock.worker :refer [start-worker]]
            [flock.func :refer [create-func]]
            [flock.task :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts
           (do
             (setup-fact)
             (create-func (func-comp) test-func1)
             (create-func (func-comp) test-func2)))
   (after :facts (teardown-test))])

(def result-task-nr1 result-task1)
(def result-task-nr2 result-task2)
(def result-task-nr3 result-task3)

(facts "create-task-test"
       (fact
         (let [task (-> (create-task (task-comp) test-task1)
                        (to-expected))]
           task  => result-task-nr1
           (:short_key task) => (:task_key task))
         (let [task (-> (create-task (task-comp) test-task2)
                        (to-expected))]
           task  => result-task-nr2
           (:short_key task) =not=> (:task_key task))))

(facts "No Resource control reserve-task and compete-task test"
       (fact
         (create-task (task-comp) test-task1)
         (create-task (task-comp) test-task2)
         (start-worker (worker-comp) "1.1.1.1" 1 "java")
         (-> (reserve-task (task-comp) 1)
             (to-expected))
         => (-> result-task-nr1
                (assoc :wid 1))

         (-> (reserve-task (task-comp) 1)
             (to-expected))
         => (into result-task-nr2 {:wid 1})

         (reserve-task (task-comp) 1) => nil

         (-> (get-task-by-id (task-comp) 1)
             (to-expected))
         => (into result-task-nr1 {:wid 1})

         (-> (get-task-by-id (task-comp) 2)
             (to-expected))
         => (into result-task-nr2 {:wid 1})

         (complete-task (task-comp) {:tid 1 :wid 1 :eta 123 :new_eta 234})
         => {:msg "rescheduled task tid=1 new_eta=234"}

         (-> (get-task-by-id (task-comp) 1)
             (to-expected))
         => (into result-task-nr1 {:eta 234 :wid 0})

         (complete-task (task-comp) {:tid 2 :wid 1 :eta 123})
         => {:msg "no new_eta, task tid=2 deleted"}

         (complete-task (task-comp) {:tid 2 :wid 1 :eta 123})
         => (throws Exception "task tid=2 is not reserved by worker wid=1")

         (get-task-by-id (task-comp) 2) => nil))


(fact "No Resource control reserve-task tests"
      (fact
        ; create another task with same resource as test-task but fid=2
        (create-task (task-comp) test-task1)
        (create-task (task-comp) test-task2)
        (create-task (task-comp) test-task3)
        (start-worker (worker-comp) "1.1.1.1" 1 "java")
        (start-worker (worker-comp) "1.1.1.1" 2 "java")
        (-> (reserve-task (task-comp) 1)
            (to-expected))
        => (-> result-task-nr1
               (assoc :wid 1))
        ; task 3 is earlier than task2. Since no resource control, should get task3
        (-> (reserve-task (task-comp) 1)
            (to-expected))
        => (-> result-task-nr3
               (assoc :wid 1))

        ; test-task3 should be throttled
        (-> (reserve-task (task-comp) 2)
            (to-expected))
        => (-> result-task-nr2
               (assoc :wid 2))

        (to-expected (get-task-by-id (task-comp) 1))
        => (into result-task-nr1 {:wid 1})

        (complete-task (task-comp) {:tid 1 :wid 1 :eta 123 :new_eta 126})
        (reset-task-cache (task-comp))
        (to-expected (reserve-task (task-comp) 1))
        => (-> result-task-nr1
               (assoc :wid 1 :eta 126))))
