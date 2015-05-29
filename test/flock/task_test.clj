(ns flock.task-test
  (:require [midje.sweet :refer :all]
            [clojure.java.jdbc :as jdbc]
            [clj-time.core :as tc]
            [flock.test-system :refer :all]
            [base.util :refer :all]
            [flock.worker :refer [start-worker]]
            [flock.func :refer [create-func]]
            [flock.task :refer :all]
            [flock.server :refer [start-server
                                            update-server-heartbeat
                                            refresh-sid-list]]
            [base.mysql :as mysql]
            [base.util :as util]))

(set-test-name *ns*)
(namespace-state-changes
  [(before :facts (do
                    (setup-fact)
                    (reset-task-cache (task-comp))
                    (update-server-heartbeat (server-comp))
                    (refresh-sid-list (server-comp))
                    (create-func (func-comp) test-func1)
                    (create-func (func-comp) test-func2)))
   (after :facts (teardown-test))])


(facts "create-task-test"
       (fact
         (let [task (-> (create-task (task-comp) test-task1)
                        (to-expected))]
           task  => result-task1
           (:short_key task) => (:task_key task))
         (let [task (-> (create-task (task-comp) test-task2)
                        (to-expected))]
           task  => result-task2
           (:short_key task) =not=> (:task_key task))

         ; delete the schedule portion
         (jdbc/delete! (db) :schedule ["tid=2"])
         ; update on creation with different value
         (->> (assoc test-task2 :params {:updated 789})
              (create-task (task-comp))
              (to-expected))
         => (assoc result-task2 :params {"updated" 789}
                                :msg "task already exists.")))

(facts "create-task-test nested params"
       (fact
         (->> (assoc test-task1
                :params {"b" "abc" "nest" {"nesta" 123}})
              (create-task (task-comp)))
         => (assoc result-task1
              :params {"b" "abc" "nest" {"nesta" 123}})))

(facts "create-task-test neg eta"
       (fact
         (->> (assoc test-task1 :eta -1)
              (create-task (task-comp))) => (throws AssertionError)
         ; currentTime is in millsec and eta is in epoch
         (->> (assoc test-task1 :eta (System/currentTimeMillis))
              (create-task (task-comp))) => (throws AssertionError)))

(facts "create-task-test no eta"
       (fact
         (->> (dissoc test-task1 :eta )
              (create-task (task-comp))) => (throws AssertionError)))

(facts "create-task-test no task_key"
       (fact
         (->> (dissoc test-task1 :task_key )
              (create-task (task-comp))) => (throws AssertionError)))

(facts "create-task-test no fid"
       (fact
         (->> (dissoc test-task1 :fid )
              (create-task (task-comp))) => (throws AssertionError)))

(facts "create-task-test duplicate fid task_key"
       (fact
         (create-task (task-comp) test-task1)
         (:msg (create-task (task-comp) test-task1))
         => "task already exists."))

(facts "get-task-by-id test"
       (fact
         (create-task (task-comp) test-task1)
         (-> (get-task-by-id (task-comp) 1)
             (to-expected)) => result-task1))

(facts "get-task-by-id test none exist"
       (fact
         (get-task-by-id (task-comp) 3) => nil))

(facts "get-task-by-fid-key test."
       (fact
         (create-task (task-comp) test-task1)
         (-> (get-task-by-fid-key (task-comp) 1 (test-task1 :task_key))
             (to-expected))
         => result-task1))

(facts "get-task-by-fid-key test none exist"
       (fact
         (get-task-by-fid-key (task-comp) 1 "none-exist-key") => nil))


(facts "get-task-by-fid-key test neg fid"
       (fact
         (get-task-by-fid-key (task-comp) -1 "no-useful key")
         => (throws AssertionError)))

(facts "get-task-by-fid-key test no key"
       (fact
         (get-task-by-fid-key (task-comp) 1 nil)
         => (throws AssertionError)))

(facts "update-task test"
       (fact
         (create-task (task-comp) test-task1)
         (Thread/sleep 1000)
         (let [t1 (get-task-by-id (task-comp) 1)
               new-param {"new_param" "new_param"}
               t2 (update-task (task-comp) 1 "http://test.domain1.com/key1" 345 new-param)]
           (to-expected t2) =>  (into result-task1
                                 {:eta 345 :params new-param
                                  :task_key "http://test.domain1.com/key1"
                                  :reverse_domain "com.domain1.test"
                                  :short_key "http://test.domain1.com/key1"})
           (< (:modified t1) (:modified t2)) => true)))

(facts "update-task test no params"
       (fact
         (create-task (task-comp) test-task1)
         (-> (update-task (task-comp) 1 "new task key 2" 345 nil)
             (to-expected))
         => (into result-task1 {:eta 345 :task_key "new task key 2"
                                :reverse_domain nil
                                :short_key "new task key 2"})))

(facts "reserve-task and compete-task test"
       (fact
         (setup-fact)
         (reset-task-cache (task-comp))
         (create-func (func-comp) test-func1)
         (create-func (func-comp) test-func2)

         (create-task (task-comp) test-task1)
         (create-task (task-comp) test-task2)
         (start-worker (worker-comp) "1.1.1.1" 1 "java")
         (-> (reserve-task (task-comp) 1)
             (to-expected))
         => (-> result-task1
                (assoc :wid 1))

         (-> (reserve-task (task-comp) 1)
             (to-expected))
         => (into result-task2 {:wid 1})

         (reserve-task (task-comp) 1) => nil

         (-> (get-task-by-id (task-comp) 1)
             (to-expected))
         => (into result-task1 {:wid 1})

         (-> (get-task-by-id (task-comp) 2)
             (to-expected))
         => (into result-task2 {:wid 1})

         (complete-task (task-comp) {:tid 1 :wid 1 :eta 123
                                     :new_eta 234 :start_time 126})
         => {:msg "rescheduled task tid=1 new_eta=234"}

         (-> (get-task-by-id (task-comp) 1)
             (to-expected))
         => (into result-task1 {:eta 234 :wid 0})

         (complete-task (task-comp) {:tid 2 :wid 1 :eta 123})
         => {:msg "no new_eta, task tid=2 deleted"}

         (complete-task (task-comp) {:tid 2 :wid 1 :eta 123})
         => (throws Exception "task tid=2 is not reserved by worker wid=1")

         (get-task-by-id (task-comp) 2) => nil))

(fact "reserve-task future eta test"
      (fact
        (create-task (task-comp) (assoc test-task1 :eta (+ 1 (current-epoch))))
        (start-worker (worker-comp) "1.1.1.1" 1 "java")
        (reserve-task (task-comp) 1) => nil
        (let [eta (- (current-epoch) 1 )]
          (create-task (task-comp) (assoc test-task2 :eta eta))
          (reset-task-cache (task-comp))
          (-> (reserve-task (task-comp) 1)
              (to-expected))
          => (-> result-task2
                 (assoc :wid 1 :eta eta)))))

(facts "delete-task"
      (fact
        (create-task (task-comp) test-task1)
        (delete-task (task-comp) 1)
        (get-task-by-id (task-comp) 1)
        => nil

        (create-task (task-comp) test-task2)
        (start-worker (worker-comp) "1.1.1.1" 2 "java")
        (-> (reserve-task (task-comp) 1)
            (to-expected))
        => (assoc result-task2 :wid 1)

        (-> (get-task-by-id (task-comp) 2)
            (to-expected))
        => (assoc result-task2 :wid 1)

        (delete-task (task-comp) 2)
        (get-task-by-id (task-comp) 1)
        => nil

        (complete-task (task-comp) {:wid 1 :tid 2 :eta 123})
        => (throws Exception "task tid=2 is not reserved by worker wid=1")))


(facts "list and count func backlog task"
       (fact
         (create-task (task-comp) test-task1)
         (create-task (task-comp) test-task2)
         (-> (list-func-backlog (task-comp) 1)
             (first)
             (to-expected)) => result-task1
         (count-func-backlog (task-comp) 1)
         => {:fid 1 :backlog_count 2}

         (start-worker (worker-comp) "1.1.1.1" 2 "java")
         (reserve-task (task-comp) 1)
         (count-func-backlog (task-comp) 1)
         => {:fid 1 :backlog_count 1}

         (->> (list-func-backlog (task-comp) 1)
              (map to-expected))
         => (list result-task2)

         (count-func-backlog (task-comp) 99)
         => {:fid 99 :error "unknown fid"}))

