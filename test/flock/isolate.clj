;; -*- coding: utf-8 -*-
;; (c)2014 Flipboard Inc, All Rights Reserved.
;; Author: Howard Zhao
;; created: 6/11/15
;; flock.isolate
;; 
;; Purpose: place holder for isolated test
;;

(ns flock.isolate
  (:require [midje.sweet :refer :all]
            [clojure.java.jdbc :as jdbc]
            [flock.test-system :refer :all]
            [base.util :refer :all]
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
                    (reset-task-cache (task-comp))
                    (update-server-heartbeat (server-comp))
                    (refresh-sid-list (server-comp))
                    (create-func (func-comp) test-func1)
                    (create-func (func-comp) test-func2)))
   (after :facts (teardown-test))])

(fact "reserve-task future eta test"
      (fact
        (create-task (task-comp) (assoc test-task1 :eta (+ 1 (current-epoch))))
        (start-worker (worker-comp) "1.1.1.1" 1 "java")
        (reserve-task (task-comp) 1) => nil
        (let [eta (- (current-epoch) 1 )]
          (create-task (task-comp) (assoc test-task2 :eta eta))
          (reset-task-cache (task-comp))
          (-> (reserve-task (task-comp) 1)
              (to-expected))
          => (-> result-task2
                 (assoc :wid 1 :eta eta)))))