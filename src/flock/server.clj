;; -*- coding: utf-8 -*-

;; Author: Howard Zhao
;; created: 4/16/15
;; flock.server
;; 
;; Purpose: represent this server instance
;;   It also keeps and updates server list and slot number of this server.
;;   Server list and slot number are used during task reservation
;;   to calculate if the task is a candidate for reservation.
;;   Update the server heartbeat periodically.
;;   Clean up when other server goes away and refresh the server.
;;   Hold state {:sid <server-slot-id> :sid-list <current sorted list of sid> :

(ns flock.server
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [base.util :as util]
            [com.stuartsierra.component :as component]
            [component.database :refer [mydb insert-row]]
            [component.core :refer [get-config-int]]
            [component.scheduler :refer [schedule-fixed-delay]]))

; public API
(defn server-state
  "returns a map indicating this server id, list of known server ids and my index
   {:sid 1 :sid-list [1 3 5] :my-index 0}"
  [comp]
  (let [state (:state comp)
        sid (:sid @state)
        sid-list (:sid-list @state)]
    {:sid sid :sid-list sid-list
     :my-index (util/index-of sid sid-list)}))

(defn take-candidate?
  "check if task candidate should be processed by this server"
  [comp tid]
  (let [{:keys [sid-list my-index]} (server-state comp)]
    (->> (count sid-list)
         (max 1)
         (rem tid)
         (= my-index))))

(defn refresh-sid-list
  [comp]
  (let [state (:state comp)
        sid (:sid @state)
        curr-sids (:sid-list @state)
        new-sids (->> "select sid from server order by sid"
                      (jdbc/query (mydb comp))
                      (map :sid))]
    (when (not= new-sids curr-sids)
      (swap! state assoc :sid-list new-sids)
      (log/info "server" sid ": server list change from"
                curr-sids "to" new-sids ))))

(defn- insert-or-get-server-id
  [comp]
  (let [ip (util/get-ip)
        pid (util/get-pid)
        sid (->> ["select sid from server where ip=? and pid=?" ip pid]
                 (jdbc/query (mydb comp))
                 (first)
                 (:sid))]
    (if sid sid (insert-row (mydb comp) :server {:ip ip :pid pid}))))

(defn- register-server
  "Called when a new server starts.
  Create a server record using current ip and pid.
  returns server created"
  ([comp]
   (let [sid (insert-or-get-server-id comp)]
     (log/info "registered server sid" sid)
     (swap! (:state comp) assoc :sid sid)
     (refresh-sid-list comp))))

(defn update-server-heartbeat
  "update server heartbeat"
  [comp]
  (let [{sid :sid} (server-state comp)
        update ["update server set heartbeat = current_timestamp() where sid = ?" sid]]
    (if (= '(1) (->> (jdbc/execute! (mydb comp) update)))
      (refresh-sid-list comp)
      (do (log/warn "server" sid "heartbeat failed. Server was presumed dead, reregister")
          (register-server comp)))))

(defn- monitor-dead-server
  "Called by monit-thread to delete dead servers"
  [comp]
  (let [heartbeat (get-config-int comp "flock.server.heartbeat" 10)
        max-skip (get-config-int comp "flock.server.max.skipped.heartbeats" 4)
        allowance (* heartbeat max-skip)
        delete ["heartbeat < now() - INTERVAL ? SECOND" allowance]]
    (log/debug "delete server with heartbeat" allowance "sec old")
    (when (not= '(0) (jdbc/delete! (mydb comp) :server delete) )
      (refresh-sid-list comp))))

(defn start-server
  "start this server and schedule maintenance task for heartbeat and dead server."
  [comp]
  (let [comp (assoc comp :state (atom {}))
        scheduler (:scheduler comp)
        monitor-cycle (get-config-int comp "flock.server.monitor.cycle.sec" 10)
        heartbeat (get-config-int comp "flock.server.heartbeat" 10)]
    (register-server comp)
    (log/info "starting server heartbeat" heartbeat "sec")
    (->> (fn [] (update-server-heartbeat comp))
         (assoc {:delay heartbeat :msg "server heartbeat"} :command)
         (schedule-fixed-delay scheduler))
    (log/info "starting server monitoring, checking every" monitor-cycle "sec")
    (->> (fn [] (monitor-dead-server comp))
         (assoc {:delay monitor-cycle :msg "monitor dead server"} :command)
         (schedule-fixed-delay scheduler ))
    comp))

; Domain model component for
(defrecord ServerComponent [core flock-db]
  component/Lifecycle
  (start [this]
    (start-server this))

  (stop [this]
    (dissoc this :state)))

(defn new-server-comp
  []
  (map->ServerComponent {}))
