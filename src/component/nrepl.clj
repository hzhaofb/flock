;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 3/17/15
;; component.nrepl
;; 
;; Purpose: enable nrepl service for remote debugging
;;

(ns component.nrepl
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.nrepl.server :as rs]
            [component.core :refer [cfg]]
            [flock.util :as util]
            [clojure.tools.logging :as log]))

(defrecord nreplComponent [core]
  component/Lifecycle

  (start [this]
    (let [port (util/get-config-int this "flock.nrepl.port" 8081)]
      (log/info "starting nrepl on port " port)
      (assoc this :nrepl (rs/start-server :port port))))

  (stop [this]
    (log/info "stopping nrepl.")
    (rs/stop-server (:nrepl this))
    (assoc this :nrepl nil)))

(defn new-nrepl
  "create a nrepl component"
  []
  (map->nreplComponent {}))