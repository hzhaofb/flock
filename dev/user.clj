(ns user
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
 (:require
  [clojure.java.io :as io]
  [clojure.java.javadoc :refer [javadoc]]
  [clojure.pprint :refer [pprint]]
  [clojure.reflect :refer [reflect]]
  [clojure.repl :refer [apropos dir doc find-doc pst source]]
  [clojure.set :as set]
  [clojure.string :as str]
  [clojure.java.shell :refer [sh]]
  [clojure.java.jdbc :as jdbc]
  [clojure.stacktrace :refer [e]]
  [clojure.tools.logging :as log]
  [clojure.test :as test]
  [clj-http.client :as client]
  [clojurewerkz.propertied.properties :as props]
  [stencil.core]
  [com.stuartsierra.component :as component]
  [midje.repl :refer :all]
  [reloaded.repl :refer [system init start stop go reset]]
  [clojure.tools.namespace.repl :refer [refresh refresh-all]]
  [cheshire.core :as json]
  [pallet.thread-expr :refer :all]
  [base.util :refer :all]
  [base.rest-util :refer :all]
  [flock.system]
  [component.database :as rdb]
  [flock.test-system :refer :all]
  [flock.worker :refer :all]
  [flock.tasklog :refer :all]
  [flock.func :refer :all]
  [base.mysql :refer :all]
  [flock.task :refer :all]
  [flock.server :refer :all])
 (:import [com.mchange.v2.c3p0.ComboPooledDataSource]))

;; don't cache templates in dev
(stencil.loader/set-cache (clojure.core.cache/ttl-cache-factory {} :ttl 0))

(reloaded.repl/set-init! flock.system/dev-system)
