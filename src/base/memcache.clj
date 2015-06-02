;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 1/12/15
;; base.memcache
;; 
;; Purpose: create memcache connection. Used by memcache component
;; TODO: remove this and memcache component

(ns base.memcache
  (:import [net.spy.memcached AddrUtil ConnectionFactory BinaryConnectionFactory
                              ConnectionFactoryBuilder ConnectionFactoryBuilder$Locator DefaultHashAlgorithm
                              ConnectionFactoryBuilder$Protocol MemcachedClient]))

(defn- ^ConnectionFactory connection-factory
  "build a connection factory that uses a binary connection and Ketama consistent-hashing"
  [timeout]
  (let [cfb (ConnectionFactoryBuilder. (BinaryConnectionFactory.))]
    (doto cfb
      (.setProtocol ConnectionFactoryBuilder$Protocol/BINARY)
      (.setHashAlg DefaultHashAlgorithm/KETAMA_HASH)
      (.setLocatorType  ConnectionFactoryBuilder$Locator/CONSISTENT)
      (.setOpTimeout timeout))
    (.build cfb)))

(defn connect
  "create a memcache connection to servers with timeout"
  [servers timeout]
  (let [factory (connection-factory (or timeout 4000))
        addresses (AddrUtil/getAddresses servers)]
    (MemcachedClient. factory addresses)))