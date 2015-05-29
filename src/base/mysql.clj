;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; base.mysql
;; functions help to manage mysql entity
(ns base.mysql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [clojure.string :refer [join]]
            [base.util :refer :all]))

(defn- to-str
  "convert keyword to column name if input is keyword"
  [x]
  (if (keyword? x) (name x)
    x))

(defn get-single-row
  "retrieves table row by id, nil if not exist"
  [db table id_col id]
  (-> (str "select * from " (to-str table) " where " (to-str id_col) "=?")
      (vector id)
      (->> (jdbc/query db) (first))))

(defn insert-row
  "insert a row to a table and returns generated id."
  [db table entity]
  (->> (jdbc/insert! db table entity)
       (first) (:generated_key)))

(defn join-table
  "Given a table result, add joined table in memory.
  The id (in key form) column need to be the same in both table."
  [db otable id results]
  (let [lookup
        ; mapping id to other table result
        (->> (map id results)
             (distinct)
             (join ",")
             (format (str "select * from " otable
                          " where " (name id) " in (%s)"))
             (jdbc/query db)
             (reduce #(assoc %1 (id %2) %2) {}))]
    (map #(merge % (lookup (id %))) results)))