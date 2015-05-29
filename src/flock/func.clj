;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; flock.func
;; func defines the method and settings of a group of tasks
;; including: fid, lang, function name, settings

(ns flock.func
  (:require [clojure.java.jdbc :as jdbc]
            [component.webservice :refer [WebService]]
            [base.util :refer :all]
            [ring.middleware.json :refer [wrap-json-body wrap-json-params wrap-json-response]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [base.rest-util :refer [json-response echo]]
            [com.stuartsierra.component :as component]
            [component.memcache :as mc]
            [flock.environment :as environ]
            [flock.util :refer [mydb mycache]]
            [base.mysql :refer :all]
            [compojure.core :as cc])
  (:import (com.mysql.jdbc.exceptions.jdbc4 MySQLIntegrityConstraintViolationException)))

(defn to-cache-key
  [fid]
  (str "flk-fn-" fid))

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
      ; also delete the cache key in case old cached
      ; (mostly for test case)
      (mc/delete-val (mycache comp) (to-cache-key fid))
      (assoc func :fid fid :eid eid))
    (catch MySQLIntegrityConstraintViolationException ex
      (-> (get-func-by-name comp (:env func) (:name func))
          (assoc :msg "func already exists:" )))))

(defn- get-func-cache
  [comp fid]
  (mc/get-val (mycache comp) (to-cache-key fid)))

(defn- get-func-db
  [comp fid]
  (let [db (mydb comp)
        cache (mycache comp)
        cache-key (to-cache-key fid)
        sql "select f.*, e.name as env
            from func f join environment e on f.eid = e.eid
            where fid=?"]
    (->> (jdbc/query db [sql fid])
         (first)
         (mc/set-val cache cache-key 60))))

(defn get-func
  [comp fid]
  (or (get-func-cache comp fid)
      (get-func-db comp fid)))

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
        (mc/delete-val (mycache comp) (to-cache-key fid))
        {:msg "func settings updated"})
      (throw (Exception. (str "func is not found for fid=" fid))))))

; expose REST endpoints
(defn- make-routes
  [comp]
  (cc/routes
    (cc/GET "/func/:fid" [fid :as req]
            ;; get function with id
            (json-response req get-func comp fid))
    (cc/POST "/func" req
             ;; create a new function with specified
             ;; eid, name, and settings
             ;; if already exist, return existing func
             (json-response req create-func comp (req :params)))
    (cc/PUT "/func/:fid" [fid settings :as req]
            ;; update function's settings
            (json-response req update-func comp fid settings))
    (cc/POST "/pfunc" req
             ;; post on this endpoint for sanity test
             (json-response req echo req))
    ))

; Declare dependencies
(defrecord FuncComponent [core flock-db]
  component/Lifecycle
  (start [this]
    (->> (make-routes this)
         (wrap-json-response)
         (assoc this :routes)))

  (stop [this]
    this)

  WebService
  (get-routes [this]
    (:routes this)))

(defn new-func-comp
  []
  (map->FuncComponent {}))