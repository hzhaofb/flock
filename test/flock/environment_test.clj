;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 5/19/15
;; flock.environment_test
;; 
;; Purpose: test cases for environment.clj
;;

(ns flock.environment-test
  (:require [midje.sweet :refer :all]
            [flock.func :refer :all]
            [flock.environment :refer :all]
            [flock.test-system :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "test-get-env-id or name"
       (fact
         (get-env-by-name (env-comp) "java")
         => 1

         (get-env-by-id (env-comp) 2)
         => "python"

         (get-env-by-name (env-comp) "cobol")
         => nil))
