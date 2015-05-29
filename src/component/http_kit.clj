;; -*- coding: utf-8 -*-
;;

;; Author: David Creemer
;;
;; component.http-kit
;;
;; inspired by https://github.com/danielsz/system -- public domain
;;

(ns component.http-kit
  (:require [com.stuartsierra.component :as component]
            [component.webservice :as ws]
            [compojure.core :as cc]
            [compojure.route :as cr]
            [component.core :refer [cfg]]
            [ring.middleware.json :refer [wrap-json-body wrap-json-params wrap-json-response]]
            [metrics.ring.expose :refer [expose-metrics-as-json]]
            [metrics.ring.instrument :refer [instrument]]
            [org.httpkit.server :refer [run-server]]
            [clojure.tools.logging :as log]
            [flock.util :as util])
 (:use [ring.middleware.keyword-params :only [wrap-keyword-params]]
       [ring.middleware.params :only [wrap-params]]
       [ring.middleware.content-type :only [wrap-content-type]]))

(def global-routes
  (cc/routes
   (cr/files "/static" {:root "public"})
   ; lein uberjar packages the resources directory in the root of uberjar
   ; Static files must be put in "resources/public/"
   ; and referred with "public" prefix, e.g.,
   ; file resources/public/images/fl-mark.svg should be referred as
   ; /resources/images/fl-mark.svg
   (cr/resources "/resources" {:root "public"})
   (cr/not-found "Not Found")))

(defn- web-service?
  [n]
  (if-let [a (ancestors (class n))]
    (contains? a component.webservice.WebService)
    false))

(defn- assemble-routes-to-app
  "assemble all of the components that implement the WebService protocol
  into an application map (in the order given)"
  [this]
  (cc/routes (->> (vals this)           ; get all dependent components
                  (filter web-service?) ; filter just those with WebService
                  (map ws/get-routes)   ; get the routes from those
                  (apply cc/routes)     ; and turn into one handler
                  (instrument)         ; wrapped with instrumentation
                  )
             global-routes))            ; and add on global routes

(defn- wrap-common-middleware
  [app]
  (-> app
      (expose-metrics-as-json)
      (wrap-keyword-params)
      (wrap-params)
      ; following only apply if request content-type is json
      (wrap-json-params)
      (wrap-content-type)))

(defrecord WebServer []
  component/Lifecycle

  (start [this]
    (let [port   (util/get-config-int this "flock.http.port" 8080)
          app    (wrap-common-middleware (assemble-routes-to-app this))
          server (run-server app {:port port :join? false})
          _      (log/info "Started webservice on port" port)]
      (assoc this :server server :port port)))

  (stop [this]
    (log/info "stopping http-kit component")
    (if-let [stop-server (:server this)]
      (stop-server))
    this))

(defn new-web-server
  []
  (map->WebServer {}))
