;; -*- coding: utf-8 -*-
;;

;; Author: David Creemer
;;
;; component encapsulation of all common "core" services

(ns component.core
  (:require [com.stuartsierra.component :as component]
            [clojure.java.io :as io]
            [clojurewerkz.propertied.properties :as props]
            [clojure.tools.logging :as log]))

(defn cfg
  "return config map for any component with core"
  [comp]
  (-> (get-in comp [:core :config])
      (deref)))

(defn set-config!
  "set config with new value on core component"
  [core new-config]
  (reset! (:config core) new-config))

;; initialize all of the "core" elements of an application
(defrecord Core [app-name]

  component/Lifecycle

  (start [this]
    (log/info "starting app" app-name)
    (->> (str app-name ".properties")
         (io/file)
         (props/load-from)
         ; need to use atom to hold the map so we can change it
         ; during test
         (atom)
         (assoc this :config)))

  (stop [this]
    this))

(defn new-core
  [app-name]
  (map->Core {:app-name app-name}))
