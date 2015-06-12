(ns flock.worker-test
  (:require [midje.sweet :refer :all]
            [flock.worker :refer :all]
            [flock.task :refer :all]
            [flock.func :refer :all]
            [flock.server :refer :all]
            [flock.test-system :refer :all]
            [clojure.tools.logging :as log]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "test worker CRUD both positive and negative cases "
       (fact
         (start-worker (worker-comp) "1.1.1.1" 1 "java")
         (select-keys (start-worker (worker-comp) "1.1.1.1" 1 "java")
                      [:ip :pid :wid] )
         => {:ip "1.1.1.1" :pid 1 :wid 1}

         (get-worker-by-id (worker-comp) 2) => nil

         (start-worker (worker-comp) nil 1 "java") => (throws AssertionError)
         (start-worker (worker-comp) "1.1.1.1" nil "java") => (throws AssertionError)
         (start-worker (worker-comp) nil -1 "java") => (throws AssertionError)
         (start-worker (worker-comp) nil 1 "cobol") => (throws AssertionError)

         (let [start ((get-worker-by-id (worker-comp) 1) :heartbeat)
               _ (Thread/sleep 500)
               worker (update-heartbeat (worker-comp) 1 "test info")]
           (select-keys worker [:ip :pid :wid :wstatus])
           => {:ip "1.1.1.1" :pid 1 :wid 1 :wstatus "test info"}
           (worker :heartbeat) => #(<= start %))

         ; test stop worker
         (stop-worker (worker-comp) 1)
         (get-worker-by-id (worker-comp) 1) => nil))

(facts "test-monitor-worker and task is release after worker expires"
       (fact
         (-> (merge test-local-config
                    {"flock.worker.monitor.cycle.sec" "1"
                     "flock.worker.heartbeat" "1"
                     "flock.worker.max.skipped.heartbeats" "1"})
             (set-test-config!))
         (start-monitor (worker-comp))
         (start-server (server-comp))
         (start-worker (worker-comp) "1.1.1.1" 2 "java")
         ; create task tid 1
         (create-func (func-comp) test-func1)
         (create-task (task-comp) test-task1)
         (reserve-task (task-comp) 1)
         ; let worker expire
         (Thread/sleep 4000)
         (let [worker (get-worker-by-id (worker-comp) 1)
               hb_resp(update-heartbeat (worker-comp) 1 nil)]
           worker => nil
           hb_resp => {:msg "worker 1 is not found"})
         ; Worker 1 expired and so task should be ready for reserve
         ; worker 2 should be able to reserve the task
         (start-worker (worker-comp) "1.1.1.2" 1 "java")
         (refresh-sid-list (server-comp))
         (reset-task-cache (task-comp))
         (-> (reserve-task (task-comp) 2)
             (to-expected))
         => (into result-task1 {:wid 2})
         (teardown-test)))


