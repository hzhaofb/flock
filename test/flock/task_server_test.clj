;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 4/22/15
;; flock.task_test2
;; 
;; Purpose:
;;
(ns flock.task-server-test
  (:require [midje.sweet :refer :all]
            [flock.test-system :refer :all]
            [base.util :refer :all]
            [component.database :refer :all]
            [flock.worker :refer [start-worker]]
            [flock.func :refer [create-func]]
            [flock.task :refer :all]
            [flock.server :refer [start-server
                                  update-server-heartbeat
                                  refresh-sid-list]]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (do
                    (setup-fact)
                    (create-func (func-comp) test-func1)
                    (create-func (func-comp) test-func2)
                    (create-func (func-comp) test-func3)))
   (after :facts (teardown-test))])

(fact  "reserve task on different server test"
       ; we need to hack another server component with different ip pid
       (let [svr-comp1 (start-server (server-comp))
             task-comp1 (assoc (task-comp) :server-comp svr-comp1)]
         (->> (insert-row (db) :server {:ip "1.1.1.1" :pid 2})
              (swap! (:state svr-comp1) assoc :sid))
         (refresh-sid-list svr-comp1)
         (refresh-sid-list (server-comp))

         (create-task (task-comp) test-task1)
         (create-task (task-comp) test-task2)
         (create-task (task-comp) test-task3)
         (start-worker (worker-comp) "1.1.1.1" 1 "java")

         (->> (reserve-tasks (task-comp) 1 10)
              (map to-expected))
         => (->> (list result-task2)
                 (map #(assoc % :wid 1)))

         (reset-task-cache task-comp1)
         (->> (reserve-tasks task-comp1 1 10)
              (map to-expected))
         => (->> (list result-task1 result-task3)
                 (map #(assoc % :wid 1)))
         (teardown-test)))



