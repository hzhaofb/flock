;; -*- coding: utf-8 -*-
;;

;; Author: Howard Zhao
;;
;; flock.main

(ns flock.main
  (:gen-class)
  (:require [com.stuartsierra.component :as component]
            [flock.system :as system]))

(def system (system/prod-system))

(defn -main
  []
  (alter-var-root #'system component/start))
