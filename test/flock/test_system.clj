;; setup test system including all components

(ns flock.test-system
  (:require [clojure.java.jdbc :as jdbc]
            [midje.sweet :refer :all]
            [flock.func :refer :all]
            [com.stuartsierra.component :as component]
            [component.database :as database]
            [component.core :as core]
            [flock.worker :as worker]
            [flock.task :as task]
            [flock.server :as server]
            [base.util :refer :all]
            [flock.func :as func]
            [component.health :as health]
            [component.http-kit :as http-kit]
            [flock.system :as system]
            [clojure.tools.logging :as log]))


(def test-local-config
  {"flock.worker.heartbeat" "1"
   "flock.worker.max.skipped.heartbeats" "1"
   "flock.worker.monitor.cycle.sec" "1000000"
   "flock.db.adapter" "mysql"
   "flock.db.port" "3306"
   "flock.db.timeout" "5000"
   "flock.db.host" "localhost"
   "flock.db.database" "flocktest"
   "flock.db.user" "root"
   "flock.db.password" "root"
   "flocklog.db.adapter" "mysql"
   "flocklog.db.port" "3306"
   "flocklog.db.timeout" "5000"
   "flocklog.db.host" "localhost"
   "flocklog.db.database" "flocklogtest"
   "flocklog.db.user" "root"
   "flocklog.db.password" "root"})

(defn test-system
  []
  (system/dev-system))

(defonce sysa (atom nil))
(defn tsys []
  (if (nil? @sysa)
    (try
      (log/info "starting test system")
      (reset! sysa (component/start (test-system)))
      (catch Exception ex
        (log/error ex "tsys start failure"))))
  @sysa)

(defn core [] (get (tsys) :core))

(defn worker-comp [] (get (tsys) :worker-comp))

(defn server-comp [] (get (tsys) :server-comp))

(defn task-comp [] (get (tsys) :task-comp))

(defn func-comp [] (get (tsys) :func-comp))

(defn env-comp [] (get (tsys) :env-comp))

(defn set-test-config!
  [config-map]
  (log/info "setting test config to " config-map)
  (core/set-config! (core) config-map))

(def test-func1 {:env "java" :name "test.class.DoSomthing1"
                 :settings "{\"test\": \"test setting\"}"})

(def test-func2 {:env "java" :name "test2.class.DoSomethig2"
                 :settings "{\"test\": \"test setting2\"}"})

(def test-func3 {:env "go" :name "test3.class.DoSomethig3"
                 :settings "{\"test\": \"test setting3\"}"})

(def test-func-expect (assoc test-func1 :eid 1))

(def settings "{\"additional\" :\"additional settings\"}")

(def test-task1 {:fid 1
                 :eta 123
                 :task_key "http://www.test.com/path1?a=1"
                 :params {"p1" 1 "p2" "test str"}})

(defn to-expected
  [task]
  (dissoc task :modified))

(def long_key
  "http://www.test2.com/path2?q=Stanley+Black+%26+Decker&a=2+1&with_longer_than_40_char_url1234567890123456789012345678901234567890")

(def test-task2 {:fid 1
                 :eta 125
                 :task_key long_key
                 :params {"p1" 2 "p2" "test str"}})

(def test-task3 (assoc test-task1 :fid 2 :eta 124))

(defn- get-short-key
  [key]
  (if (< (count key) 41)
    key
    (get-sha1-hash key)))

(defn- to-result
  ([task testid]
   (-> (assoc task
         :short_key (get-short-key (:task_key task))
         :reverse_domain (get-reverse-domain (:task_key task))
         :wid 0
         :eid 1
         :tid testid)))
  ([task] (to-result task (:tid task))))

(def result-task1 (to-result test-task1 1))
(def result-task2 (to-result test-task2 2))
(def result-task3 (to-result test-task3 3))

(def test-task-list
  (->> (range 1 11)
       (map #(assoc {:fid 3}
              :tid %
              :eta %
              :params {"test param" %}
              :task_key (str "http://www.test" % ".com/testpath")))))

(def test-task-result-list
  (map to-result test-task-list))

; conveneint methods
(defn db []
  (database/get-conn (get (tsys) :flock-db)))

(def test-name-atom (atom nil))
(defn set-test-name
  [tname]
  (reset! test-name-atom tname))

(defn setup-fact
  []
  (log/info "setup-fact for " @test-name-atom)
  ; clean up flocktest on test begin so if fails, we can check db
  ; Can't truncate server here since this is called after system start
  ; because we are using (db) which starts the tsys
  (doseq [table ["server" "worker" "worker_log" "task"
                 "schedule" "func"]]
    (jdbc/execute! (db) [(str "truncate " table)])
    (log/info "truncated" table))
  (server/start-server (server-comp))
  (set-test-config! test-local-config))

(defn teardown-test
  ;nothing for now
  []
  (log/info "teardown-test for " @test-name-atom)
  (if (some? @sysa)
    (do (log/info "teardown truncate server")
        (jdbc/execute! (db) ["truncate server"])
        (component/stop @sysa)
        (reset! sysa nil))))

; for integration testing vars
(def url
  (->> (get-in (tsys) [:http-server :port])
       (str "http://localhost:")))

(def tworker {:ip "1.1.1.1" :pid 2 :env "java"})

(defn keyify
  "convert map string key to keyword, assuming keys are string of valid keyword"
  [m]
  (into {} (for [[k v] m]
             [(keyword k) v])))

