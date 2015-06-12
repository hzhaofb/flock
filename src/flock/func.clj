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
            [component.database :refer [mydb]]))

(defn- get-func-by-name
  "get function by env and name"
  [comp env name]
  (let [eid (environ/get-env-by-name (get comp :env-comp) env)]
    (->> ["select * from func where eid = ? and name = ?" eid name]
         (jdbc/query (mydb comp))
         (first))))

(defn create-func
  "create a function with :env :name."
  [comp {env :env name :name :as func}]
  (assert-some func [:env :name])
  (let [eid (-> (get comp :env-comp)
                (environ/get-env-by-name env))
        _ (assert (some? eid) "invalid env")
        existing (get-func-by-name comp env name)]
    (if existing
      (assoc existing :msg "func already exists:" )
      (->> (assoc (dissoc func :env) :eid eid)
           (jdbc/insert! (mydb comp) :func)
           (first)
           (:generated_key)
           (assoc func :eid eid :fid)))))

(defonce func-cache (create-ttl-cache 5000))

(defn get-func
  [comp fid]
  (or (get-cache-value func-cache fid)
      (->> ["select f.*, e.name as env
             from func f join environment e on f.eid = e.eid
             where fid=?" fid]
           (jdbc/query (mydb comp))
           (first)
           (set-cache-value! func-cache fid))))

(defn update-func
  "can only update settings."
  [comp fid settings]
  (let [st (if (some? settings) (str settings))
        db (mydb comp)]
    (if (= '(1) (jdbc/update! db :func {:settings st} ["fid=?" fid]))
      (do (set-cache-value! func-cache fid nil)
          {:msg "func settings updated"})
      (throw (Exception. (str "func is not found for fid=" fid))))))

(defrecord FuncComponent [core flock-db]
  component/Lifecycle
  (start [this]
    this)

  (stop [this]
    this))

(defn new-func-comp
  []
  (map->FuncComponent {}))