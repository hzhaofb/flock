;; -*- coding: utf-8 -*-
;; Author: Howard Zhao
;; created: 6/10/15
;; .core
;; 
;; Purpose: main for flock worker implementation
;;
(ns core
  (:require [base.util :as util]
            [base.rest-util :as rest]
            [executor :refer [execute]]
            [clojure.java.io :as io]
            [clojurewerkz.propertied.properties :as props]))

; atom indicating that this process needs to shutdown
(defonce shutdown (atom false))

; properties
(def myprop (-> "worker.properties"
                (io/file)
                (props/load-from)))

(def conf
  {:server  (get myprop "server.url")
   :worker-heartbeat (util/to-int (get myprop "worker.heartbeat.secs"))
   :no-task-sleep (util/to-int (get myprop "no-task.sleep.secs"))
   :tcount  (util/to-int (get myprop "thread.count"))})

(defn- process-one
  [wid]
  (let [{tid :tid eta :eta :as task}
        (rest/rest-get (conf :server) "worker" wid "task")]
    (if tid
      (do
        (println "got task" task "sleep to simulate processing")
        (try
          (->> (execute task)
               (assoc task :new_eta)
               (rest/rest-put)))
        )

      )
    (Thread/sleep (conf :sleepms))
    (if tid
      ; adding 24 hours from now
      (->> {:new_eta (+ (util/current-epoch) (* 60 60 24))}
           (rest/rest-put (conf :server) "worker" wid "task" tid)))))

(defn process
  "repeatedly reserve task, sleep 1 sec and release task "
  [wid]
  (println "start processing")
  (loop []
    (if @shutdown
      (println "processor exits")
      (do
        (try
          (process-one wid)
          (catch Exception ex
            (println "got error " ex)))
        (recur)))))

(defn start
  []
  (let [worker  {:ip (util/get-ip) :pid (util/get-pid) :env "java"}
        {wid :wid msg :msg} (rest/rest-post (conf :server) "worker" worker)]
    (assert (some? wid) (str "work failed to register: " msg))
    (dotimes [_ (conf :tcount)]
      (future (process wid)))
    ; update the heartbeat every 5 secs
    (loop []
      (if @shutdown
        (do
          (rest/rest-delete (conf :server) "worker" wid)
          (println "worker main thread exits"))
        (let [{admin_cmd :admin_cmd}
              (rest/rest-put (conf :server) "worker" wid nil)]
          (when (= admin_cmd "SHUTDOWN")
            (reset! shutdown true))
          (println "worker heartbeat")
          (-> (myprop :worker-heartbeat)
              (* 1000)
              (Thread/sleep))
          (recur))))))

(defn -main
  "Start worker process with number of thread
    $ java -jar flocker-0.1.0-standalone.jar"
  []
  (start))

