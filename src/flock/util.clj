;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 5/20/15
;; flock.util
;; 
;; Purpose: some common utils for other components
;;

(ns flock.util
  (:require [component.core :refer [cfg]]
            [component.rds :as rdb]
            [base.util :as util]))

(defn mydb
  "can be used by a component which has :flock-db dependency"
  [comp]
  (rdb/get-conn (get comp :flock-db)))

(defn mylogdb
  [comp]
  (rdb/get-conn (get comp :flocklog-db)))

(defn myreplicadb
  "can be used by a component which has :flockreplica-db dependency"
  [comp]
  (rdb/get-conn (get comp :flockreplica-db)))

(defn mycache
  [comp]
  (get comp :memcache))

(defn get-config-int
  [comp name default]
  (-> (cfg comp)
      (get name default)
      (util/to-int)))