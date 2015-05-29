;; -*- coding: utf-8 -*-
;;

;; Author: David Creemer
;;
;; component.web-service
;; protocol that all web services must implement

(ns component.webservice)

(defprotocol WebService
  "abstraction for web services"

  (get-routes
    [this]
    "return a compojure routes map"))
