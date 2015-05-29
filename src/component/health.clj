;; -*- coding: utf-8 -*-
;;

;; Author: David Creemer
;;
;; component.health
;; a component that provides the basic service health routes and implementation


(ns component.health
  (:require [compojure.core :as cc]
            [clojure.java.io :refer [as-file]]
            [component.webservice :refer [WebService]]
            [com.stuartsierra.component :as component]
            [clojure.java.io :as io]))

(defn- response
  [body]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body body})

(defn- ping-handler
  [health]
  (swap! (:request-count health) inc)   ; incr the health request count
  (response (str "OK " (deref (:request-count health)))))

(defn- version-handler []
  (some-> (io/resource "git-hash.txt")
          (slurp)
          (response)))

(defn- make-routes [this]
  (cc/routes
    (cc/GET "/ping" [] (ping-handler this))
    (cc/GET "/int/system/health" [] (ping-handler this))
    (cc/GET "/health" [] (ping-handler this))
    (cc/GET "/version" [] (version-handler))))

(defrecord HealthService []

  component/Lifecycle

  (start [this]
    (assoc this :routes (make-routes this)))

  (stop [this]
    this)

  WebService
  (get-routes [this]
    (:routes this)))


(defn new-health-service
  []
  (map->HealthService {:request-count (atom 0)}))
