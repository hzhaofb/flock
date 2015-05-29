(ns flock.http-server-test
  (:require [midje.sweet :refer :all]
            [flock.test-system :refer :all]
            [base.rest-util :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "test post parameters"
       (fact
         (get-in (rest-post url "ptask" {:test "test ptask"})
                 [:params :test])
         => "test ptask"

         (get-in (rest-post url "pfunc" {:test "test func"})
                 [:params :test])
         => "test func"

         (get-in (rest-post url "pworker" {:test "test worker"})
                 [:params :test])
         => "test worker"

         (get-in (rest-post url "ptasklog" {:test "test tasklog"})
                 [:params :test])
         => "test tasklog"))
