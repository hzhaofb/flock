;; -*- coding: utf-8 -*-
;; create system for unit test
;;

(ns component.system
  (:require [com.stuartsierra.component :as component]
            [component.core :refer :all]
            [component.memcache :refer :all]
            [clojure.tools.logging :as log]))

(defrecord TestCore[config]
  component/Lifecycle
  (start [this]
    this)
  (stop [this]
    this))

(defn test-system
  []
  (component/system-map
    :core (new-core "flocktest")
    :memcache (component/using (new-memcached)
                               [:core])
    ))

(defonce sysa (atom nil))
(defn tsys []
  (if (nil? @sysa)
    (try
      (log/info "starting test system")
      (reset! sysa (component/start (test-system)))
      (catch Exception ex
        (log/error "tsys starrt failure" (.getMessage ex)
                   "cause:" (.getCause ex)))))
  @sysa)

(defn stop-sys
  []
  (try
    (component/stop (tsys))
    (catch Exception ex
      (log/error "tsys stop failure" (.getMessage ex)
                 "cause:" (.getCause ex))))
  (reset! sysa nil))

