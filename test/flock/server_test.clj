;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 4/20/15
;; flock.server_test
;; 
;; Purpose: Test server component
;;
(ns flock.server-test
  (:require [midje.sweet :refer :all]
            [component.core :refer [set-config!]]
            [clojure.java.jdbc :as jdbc]
            [flock.server :refer :all]
            [flock.test-system :refer :all]
            [component.database :refer [mydb insert-row]]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "test-monitor-server and server expiration"
       (fact
         ; start server
         (-> (merge test-local-config
                    {"flock.server.monitor.cycle.sec" "1"
                     "flock.server.heartbeat" "1"
                     "flock.server.max.skipped.heartbeats" "1"})
             (set-test-config!))
         (jdbc/execute! (db) ["truncate server"])
         (insert-row (db) :server {:ip "1.1.1.1" :pid 1})
         (insert-row (db) :server {:ip "1.1.1.1" :pid 2})
         (let [comp (start-server (server-comp))]
           (get-slot-info comp) => {:slot 2 :server-count 3}
           ; after 3.5 seconds, the other two server should be cleaned up
           ; and the active server should pick up the change
           (Thread/sleep 3500)
           (get-slot-info comp) => {:slot 0 :server-count 1}))
       (teardown-test))

