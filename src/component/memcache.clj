;; -*- coding: utf-8 -*-
;;

;; Author: David Creemer
;;
;; component.memcache
;;
;; manages memcache client
;; todo remove.

(ns component.memcache
  (:require [base.memcache :as mc]
            [component.core :refer [cfg]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [base.util :as util]))

(defn- new-connection
  "create a new memcached connection using config on the component."
  [comp]
  (let [timeout (-> (get (cfg comp) "memcached.timeout" 2000)
                    (util/to-int))
        servers (get (cfg comp) "memcached.hosts")]
    (log/info "memcache connecting to" servers)
    (mc/connect servers timeout)))

;
; public interface
;
(defn get-client
  "returns the memcache client in this component"
  [comp]
  (deref (:connection comp)))

(defn get-val
  "convenient method to get a value with key"
  [comp key]
  (-> (get-client comp)
      (.get key)))

(defn delete-val
  "convenient method to delete a key"
  [comp key]
  (-> (get-client comp)
      (.delete key)))

(defn set-val
  "convenient method to set a value on key with timeout
  expires in seconds.
  returns val for threading convenience"
  [comp key expires val]
  (if (nil? val)
    (delete-val comp key)
    (-> (get-client comp)
        (.set key expires val)))
  val)

; component related
(defrecord MemcachedConnection [core]
  component/Lifecycle

  (start [this]
    (log/info "starting memcache component")
    (->> (new-connection this)
         (atom)
         (assoc this :connection)))

  (stop [this]
    (.shutdown (get-client this))
    (log/info "stopped memcache component")
    (assoc this :connection nil)))

(defn new-memcached
  "create a new memcached component with config properties
  memcached.hosts
  memcached.timeout"
  []
  (map->MemcachedConnection {}))
