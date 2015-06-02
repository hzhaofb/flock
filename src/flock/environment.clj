;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 5/19/15
;; flock.environment
;; 
;; Purpose: look up environment by eid or name
;;   Store in memcache for performance
;;todo  remove memcache

(ns flock.environment
  (:require [com.stuartsierra.component :as component]
            [component.memcache :as mc]
            [clojure.java.jdbc :as jdbc]
            [flock.util :refer [mydb mycache]]
            [clojure.set :as set]))

(defn get-envs
  "get all envs as map of eid to env name {:eid 1 :env \"java\"} "
  [comp]
  (let [mem-key "flock-envs"]
    (or (mc/get-val (mycache comp) mem-key)
        (->> (jdbc/query (mydb comp) ["select eid, name from environment"])
             (reduce #(assoc %1 (:eid %2) (:name %2)) {})
             (mc/set-val (mycache comp) mem-key 600)))))

(defn get-env-by-id
  [comp eid]
  (-> (get-envs comp)
      (get eid)))

(defn get-env-by-name
  [comp name]
  (-> (get-envs comp)
      (set/map-invert)
      (get name)))

(defrecord EnvComponent [core flock-db memcache]
  component/Lifecycle
  (start [this]
    this)
  (stop [this]
    this))

(defn new-env-comp
  []
  (map->EnvComponent {}))
