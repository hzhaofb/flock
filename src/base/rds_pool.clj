;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 1/12/15
;; base.rds_pool
;; 
;; Purpose: create connection pool to mysql/rds
;;

(ns base.rds-pool
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource))
  (:require [base.util :refer [to-int]]))

(defn- get-jdbc-url
  [spec]
  (str "jdbc:" (:dbtype spec) "://"
       (:host spec) ":" (:port spec)
       "/" (:dbname spec)))

(defn create-pool
  "create connection with spec with
  {:host host
     :dbtype dbtype
     :port port
     :dbname database
     :user user
     :password password
     :max-idle-time-exccess-connections max-idle-time-exccess-connections
     :max-idle-time max-idle-time} "
  [spec]
  (let [_ (. Class (forName "com.mysql.jdbc.Driver"))
        cpds (doto (ComboPooledDataSource.)
               (.setDriverClass "com.mysql.jdbc.Driver")
               (.setJdbcUrl (get-jdbc-url spec))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections
                 (to-int (:max-idle-time-exccess-connections spec)))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime
                 (to-int (:max-idle-time spec))))]
    {:datasource cpds}))
