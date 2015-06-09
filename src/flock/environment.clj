;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 5/19/15
;; flock.environment
;; 
;; Purpose: look up environment by eid or name


(ns flock.environment
  (:require [com.stuartsierra.component :as component]
            [clojure.java.jdbc :as jdbc]
            [base.util :refer [create-ttl-cache get-cache-value set-cache-value!]]
            [component.database :refer [mydb]]
            [clojure.set :as set]))

(def env-cache (create-ttl-cache 5000))

(defn get-envs
  "get all envs as map of eid to env name {1 \"java\", 2 \"python\"} "
  [comp]
  (or (get-cache-value env-cache :envs)
      (->> (jdbc/query (mydb comp) ["select eid, name from environment"])
           (reduce #(assoc %1 (:eid %2) (:name %2)) {})
           (set-cache-value! env-cache :envs))))

(defn get-env-by-id
  [comp eid]
  (-> (get-envs comp)
      (get eid)))

(defn get-env-by-name
  [comp name]
  (-> (get-envs comp)
      (set/map-invert)
      (get name)))

(defrecord EnvComponent [core flock-db]
  component/Lifecycle
  (start [this]
    this)
  (stop [this]
    this))

(defn new-env-comp
  []
  (map->EnvComponent {}))
