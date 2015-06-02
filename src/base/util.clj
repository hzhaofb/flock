;; -*- coding: utf-8 -*-
;;

;; base.util
;; general utility functions


(ns base.util
  (:require [clojure.java.io :refer [as-url]]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.core.cache :as cache])
  (:import (java.security MessageDigest)
           (java.net InetAddress)
           (java.lang.management ManagementFactory)))

(defn get-sha1-hash
  "returns sha1 hash hex string for input data"
  [data]
  (let [di (.. (MessageDigest/getInstance "sha1")
               (digest (.getBytes data)))]
    (. (BigInteger. 1 di) toString 16)))

(defn assert-some
  "assert that the value of a map for keys ks are not nil"
  [m ks]
  (doseq [k ks]
    (assert (some? (m k)) (str "invalid " k))))

(defn assert-pos
  "assert that the value of keys ks in m is all positive number"
  ([m] (assert-pos m (keys m)))
  ([m ks]
  (assert-some m ks)
  (doseq [[k v] (select-keys m ks)]
    (assert (pos? v) (str "invalid " k)))))

(defn to-int-default
  ([val default]
   (try
     (Integer. val)
     (catch Exception e
       default))))

(defn to-int
  "parse input to integer."
  ([val name]
    (try (Integer. val)
         (catch Exception e
           (throw (Exception. (str "Invalid " name ". Integer expected."))))))
  ([val]
    (to-int val val)))

(defn get-reverse-domain
  "parse url and return inverse domain. nil if not valid url"
  [u]
  (try
    (-> (as-url u)
        (.getHost)
        (string/split #"\.")
        (reverse)
        (->> (string/join ".")))
    (catch Exception ex
      nil)))

(defn try-update
  "For input map m, try to apply function f to value of k in m
  and returns updated map. If anything fails, returns m."
  [m k f]
  (cond (nil? m) m
        (nil? (m k)) m
        :else
        (try
          (assoc m k (f (m k)))
          (catch Throwable ex m))))

(defn current-epoch
  "return the current epoch time"
  [] (quot (System/currentTimeMillis) 1000))

(defn trunc
  "trucate string n upto a given number n"
  [s n]
  (subs s 0 (min (count s) n)))

(defn get-pid
  "get the current process pid"
  []
  (-> (ManagementFactory/getRuntimeMXBean)
      (.getName) (string/split #"@") (first)(Integer.)))

(defn get-ip
  "get the current process ip address of this host"
  []
  (-> (InetAddress/getLocalHost)
      (.getHostAddress)))

(defn dequeue!
  "Given a ref of PersistentQueue, pop the queue and change the queue"
  [queue-ref]
  (dosync
    (let [val (peek @queue-ref)]
      (alter queue-ref pop)
      val)))

(defmacro log-time
  "Similiar to clojure.core/time but log/info the output with log message
   generated by applying f e.g., #(str \"doing something got \" %)
    to the value of expr. Returns the value of expr."
  [log-level result-to-msg & expr]
  `(let [start# (. System (nanoTime))
         ret# ~@expr
         duration# (-> (. System (nanoTime))
                       (- start#)
                       (double)
                       (/ 1000000.0)
                       (->> (format "dt=%.3fms")))]
     (log/logp ~log-level (~result-to-msg ret#) duration#)
     ret#))

(defmacro log-time-info
  "Similiar to clojure.core/time but log/info the output with log message
   generated by applying result-to-msg e.g., #(str \"doing something got \" %)
    to the value of expr. Returns the value of expr."
  [result-to-msg & expr]
  `(log-time :info ~result-to-msg ~@expr))

(defn indexes-of
  "return a lazy-seq of index of e in coll."
  [e coll]
  (keep-indexed #(if (= e %2) %1) coll))

(defn index-of
  "return the first index of e in collection, nil if not found"
  [e coll]
  (-> (indexes-of e coll)
      (first)))

(defn get-int-list
  "read list of ints from config map in form of property-name=1,2,4"
  [conf name]
  (->> (str/split (get conf name) #"[, ]")
       (filter not-empty)
       (map to-int)))

; local TTL cache using clojure.core.cache and provide easier interface
(defn create-ttl-cache
  "create an atom with a ttl cache. ttl in milliseconds"
  [ttl]
  (atom (cache/ttl-cache-factory {} :ttl ttl)))

(defn get-cache-value
  "lookup key value from cache-atom"
  [ac key]
  (cache/lookup @ac key))

(defn set-cache-value!
  "update the atom cache with key value. returns value"
  [ac key value]
  (swap! ac assoc key value)
  value)
