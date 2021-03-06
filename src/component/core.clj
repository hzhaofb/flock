;; -*- coding: utf-8 -*-
;; Author: David Creemer
;; component encapsulation application name (:app_name) and associated config map (:config)

(ns component.core
  (:require [com.stuartsierra.component :as component]
            [clojure.java.io :as io]
            [clojurewerkz.propertied.properties :as props]
            [clojure.tools.logging :as log]
            [base.util :as util]))

(defn cfg
  "return config map for any component with core"
  [comp]
  (-> (get-in comp [:core :config])
      (deref)))

(defn get-config-int
  [comp name default]
  (-> (cfg comp)
      (get name default)
      (util/to-int)))

(defn set-config!
  "set config with new value on core component"
  [core new-config]
  (reset! (:config core) new-config))

(defn- load-config
  [app-name]
  (->> (str app-name ".properties")
       (io/file)
       (props/load-from)
       ; need to use atom to hold the map so we can change it
       ; during test for set-config!
       (atom)))

(defrecord Core [app-name]
  component/Lifecycle

  (start [this]
    (log/info "starting app" app-name)
    (assoc this :config (load-config app-name)))

  (stop [this]
    this))

(defn new-core
  [app-name]
  (map->Core {:app-name app-name}))
