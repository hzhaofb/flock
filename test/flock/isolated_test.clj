;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 5/21/15
;; flock.temp_test
;; 
;; Purpose: for debugging tests
;;

(ns flock.isolated-test
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

