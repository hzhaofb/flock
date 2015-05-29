;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 12/18/14
;; test.memcache_test
;; 
;; test memcache component
;;

(ns component.memcache-test
  (:require [midje.sweet :refer :all]
            [component.system :refer :all]
            [component.memcache :refer :all]))

(facts "test memcache get and set"
       (fact
         (let [mc (:memcache (tsys))
               key1 "memcache-test-key1"
               key2 "memcache-test-key2"
               val1 {:k "val1"}]
           (get-val mc key1)
           => nil

           (set-val mc key1 1 val1)
           (get-val mc key1)
           => val1
           (Thread/sleep 1500)
           (get-val mc key1)
           => nil

           (set-val mc key2 10 "value tobe deleted")
           (delete-val mc key2)
           (get-val mc key2)
           => nil
           )))
