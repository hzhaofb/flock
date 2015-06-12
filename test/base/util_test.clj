(ns base.util-test
  (:require [midje.sweet :refer :all]
            [cheshire.core :as json]
            [base.util :as fu])
  (:import (clojure.lang PersistentQueue)))

(facts "assert-some"
       (fact
         (fu/assert-some {:a 1 :b 2} [:a :b]) => nil
         (fu/assert-some {} []) => nil
         (fu/assert-some {:a nil :b 2} [:a :b]) => (throws AssertionError)))

(facts "assert-pos"
       (fact
         (fu/assert-pos {:a 1 :b 2} ) => nil
         (fu/assert-pos {}) => nil
         (fu/assert-pos {:a nil :b -2}) => (throws AssertionError)))

(facts "trunc string test"
       (fact
         (fu/trunc "1234567890" 5) => "12345"
         (fu/trunc "1234567890" 10) => "1234567890"))

(facts "try-update test"
       (fact
         (fu/try-update nil :a nil) => nil
         (fu/try-update {} :a identity) => {}
         (fu/try-update {:a 1} :b read-string) => {:a 1}
         (fu/try-update {:a "1"} :a read-string) => {:a 1}
         (fu/try-update {:a "some string"} :a read-string) => {:a 'some}
         (fu/try-update {:a "invalid json"} :a json/parse-string)
         => {:a "invalid json"}
         (fu/try-update {:a "{\"int1\":1, \"str1\": \"string val\"}"}
                        :a json/parse-string)
         => {:a {"int1" 1 "str1" "string val"}}
         ))

(facts "test dequeue! with ref"
       (fact
         (let [q (-> (conj PersistentQueue/EMPTY 1 2 3 4 5)
                     (ref))]
           (fu/dequeue! q) => 1
           (seq @q) => '(2 3 4 5))))