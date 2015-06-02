(ns flock.func-test
  (:require [midje.sweet :refer :all]
            [clojure.java.jdbc :as jdbc]
            [base.util :as util]
            [flock.func :refer :all]
            [flock.environment :refer :all]
            [flock.test-system :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "test-create-func"
       (fact (create-func (func-comp) test-func1)
             => (assoc test-func-expect :fid 1)))

(facts "test-create-func no env"
       (fact (->>  (assoc test-func1 :env nil)
                   (create-func (func-comp)))
             => (throws AssertionError)))

(facts "test-create-func no method"
       (fact (->> (dissoc test-func1 :name)
                  (create-func (func-comp)))
             => (throws AssertionError)))

(facts "test-create-func java no class"
       (fact (->> (dissoc test-func1 :name)
                  (create-func (func-comp)))
             => (throws AssertionError)))

(facts "get-func"
       (fact
         (create-func (func-comp) test-func1)
         (-> (get-func (func-comp) 1)
             (dissoc :modified))
         => (assoc test-func-expect :fid 1)))

(facts "update func"
       (fact (create-func (func-comp) test-func1)
             (update-func (func-comp) 1 settings)
             => {:msg "func settings updated"}
             (-> (get-func (func-comp) 1)
                 (dissoc :modified))
             => (into test-func-expect {:fid 1 :settings settings})))

(facts "valid tid test caching"
       (fact
         (util/set-cache-value! func-cache 1 nil)
         (util/set-cache-value! func-cache 2 nil)
         (create-func (func-comp) test-func1)
         (-> (get-func (func-comp) 1)
             (dissoc :modified))
         => (assoc test-func-expect :fid 1)

         (get-func (func-comp) 2) => nil

         ; test memoize by removing the func from table
         (jdbc/delete! (db) :func ["fid=?" 1])
         (-> (get-func (func-comp) 1)
             (dissoc :modified))
         => (assoc test-func-expect :fid 1)))
