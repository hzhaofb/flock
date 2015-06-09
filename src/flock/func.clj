;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; flock.func
;; func defines the method and settings of a group of tasks
;; including: fid, lang, function name, settings
(ns flock.func
  (:require [clojure.java.jdbc :as jdbc]
            [base.util :refer :all]
            [ring.middleware.json :refer [wrap-json-body wrap-json-params wrap-json-response]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [base.rest-util :refer [json-response echo]]
            [com.stuartsierra.component :as component]
            [flock.environment :as environ]
            [component.database :refer [mydb]])
  (:import (java.sql SQLException)))

(defn- get-func-by-name
  "get function by env and name"
  [comp env name]
  (let [eid (environ/get-env-by-name (get comp :env-comp) env)]
    (->> ["select * from func where eid = ? and name = ?" eid name]
         (jdbc/query (mydb comp))
         (first))))

(defn create-func
  "create a function with :env :name."
  [comp func]
  (assert-some func [:env :name])
  (try
    (let [env-name (func :env)
          eid (environ/get-env-by-name (get comp :env-comp) env-name)
          _ (assert (some? eid) "invalid env")
          fid (->> (assoc (dissoc func :env) :eid eid)
                   (jdbc/insert! (mydb comp) :func)
                   (first)
                   (:generated_key))]
      (assoc func :fid fid :eid eid))
    (catch SQLException ex
      (-> (get-func-by-name comp (:env func) (:name func))
          (assoc :msg "func already exists:" )))))

(def func-cache (create-ttl-cache 5000))

(defn- get-func-db
  [comp fid]
  (let [db (mydb comp)
        sql "select f.*, e.name as env
            from func f join environment e on f.eid = e.eid
            where fid=?"]
    (->> (jdbc/query db [sql fid])
         (first))))

(defn get-func
  [comp fid]
  (or (get-cache-value func-cache fid)
      (->> (get-func-db comp fid)
           (set-cache-value! func-cache fid))))

(defn update-func
  "can only update settings."
  [comp fid settings]
  (let [st (if (some? settings) (str settings))
        db (mydb comp)
        res (jdbc/update!
              db :func {:settings st}
              ["fid=?" fid])]
    (if (= '(1) res)
      (do
        (set-cache-value! func-cache fid nil)
        {:msg "func settings updated"})
      (throw (Exception. (str "func is not found for fid=" fid))))))

; Declare dependencies
(defrecord FuncComponent [core flock-db]
  component/Lifecycle
  (start [this]
    this)

  (stop [this]
    this))

(defn new-func-comp
  []
  (map->FuncComponent {}))