;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 2/28/15
;; base.html_util
;; 
;; Purpose: helps to render html
;;

(ns base.html-util
  (:require [clostache.parser :as cs]
            [clojure.tools.logging :as log]
            [ring.util.response :as resp]
            [clojure.stacktrace :as st]
            [clojure.java.io :as io]))

(defn- read-partial
  "read partial content in partials/pname.mustache and associate to map with keywordified name"
  [data m pname]
  (->> (str "partials/" (name pname) ".mustache")
       (io/resource)
       (slurp)
       (#(cs/render % data))
       (assoc m (keyword pname))))

(defn- render-page
  "parse template for {{> partial}} and replace it with content of
  file in partials/<name>.mustache"
  [template data]
  (->> (io/resource template)
       (slurp)
       (re-seq #"\{\{\>\s*([\w-]*)\s*\}\}")
       (map second)
       (reduce #(read-partial data %1 %2) {})
       (cs/render-resource template data)))

(defn html-response
  "Takes a template file name (in resources) and function
   and a list of params,
   apply the function and create to ring response for html type.
   If the function result map contains :partials whose value is
   a list of mustashe names (in keywords),
   its used to replace template {{> partial}}"
  [template f & args]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (try
           (->> (apply f args)
                (render-page template))
           (catch Exception ex
             (log/error (with-out-str (st/print-stack-trace ex 50)))
             (.getMessage ex)))})
