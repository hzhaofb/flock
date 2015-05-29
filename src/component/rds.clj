;; -*- coding: utf-8 -*-
;;

;; Author: David Creemer
;;
;; component.jdbc
;;
;; RDS component encapsulate connection.
;; dbname is used to lookup config following values
;; - dbname.rds.adaptor
;; - dbname.rds.host
;; - dbname.rds.port
;; - dbname.rds.database name
;; - dbname.rds.user
;; - dbname.rds.password
;; following optional config for c3p0 connection pool
;; - dbname.rds.max.idle.time.excess.connections
;; - dbname.rds.max.idle

(ns component.rds
  (:require [com.stuartsierra.component :as component]
            [component.core :refer [cfg]]
            [clojure.tools.logging :as log]
            [base.rds-pool :as pool]
            [base.util :refer [to-int]]))

(defn- get-conf-val
  ([dbname conf key]
    (get conf (str dbname ".rds." key) nil))
  ([dbname conf key not-found]
    (get conf (str dbname ".rds." key) not-found)))

(defn- get-db-spec
  [dbname conf]
  (let [dbtype (get-conf-val dbname conf "adapter")
        host (get-conf-val dbname conf "host")
        port (get-conf-val dbname conf "port")
        database (get-conf-val dbname conf "database")
        user (get-conf-val dbname conf "user")
        password (get-conf-val dbname conf "password")
        max-idle-time-exccess-connections
        (get-conf-val dbname conf
                      "max.idle.time.excess.connections" (str (* 30 60)))
        max-idle-time
        (get-conf-val dbname conf "max.idle.time" (str (* 3 60 60)))]
    {:host host
     :dbtype dbtype
     :port port
     :dbname database
     :user user
     :password password
     :max-idle-time-exccess-connections max-idle-time-exccess-connections
     :max-idle-time max-idle-time}))


(defn get-conn
  "get the connection from RDSComponent that can be used in queries.
   User of this component should only invoke on this public API."
  [rdscomp]
  (get rdscomp :conn))

(defrecord RDSComponent [dbname core conn]
  component/Lifecycle

  (start [this]
    (let [spec (get-db-spec dbname (cfg this))
          conn (pool/create-pool spec)]
      (log/info (:dbname this) "connected to"
                (dissoc spec :password))
      (assoc this :conn conn)))

  (stop [this]
    (if-let [c (get-conn this)]
      (.close (:datasource c)))
    (log/info "stopped RDScomponent" dbname)
    (assoc this :conn nil)))

(defn new-database
  "create a new database component for a given db name. "
  [dbname]
  (log/info "Creating new db" dbname)
  (map->RDSComponent {:dbname dbname}))