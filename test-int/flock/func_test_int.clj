(ns flock.func-test-int
  (:require [midje.sweet :refer :all]
            [base.rest-util :refer :all]
            [flock.test-system :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "task and func integration test "
       (fact (->> (rest-post url "func" test-func1)
                  (:fid))
             => 1

             (-> (rest-get url "func" 1)
                 (dissoc :modified))
             => (into test-func1 {:fid 1 :status "OK" :eid 1})

             (->> (rest-put url "func" 1 {:settings "test setting update"})
                  (:msg))
             => "func settings updated"

             ; not found
             (-> (rest-get url "func" 99)
                 (:status))
             => "Not Found"

             ; set settings to null
             (-> (rest-put url "func" 1 {:settings {:setting2 "test setting 2"}})
                 (:msg))
             => "func settings updated"

             (-> (rest-get url "func" 1)
                 (:settings))
             => "{:setting2 \"test setting 2\"}"))
