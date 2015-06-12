(ns flock.worker-test-int
  (:require [midje.sweet :refer :all]
            [flock.test-system :refer :all]
            [base.rest-util :refer :all]))

(set-test-name *ns*)

(namespace-state-changes
  [(before :facts (setup-fact))
   (after :facts (teardown-test))])

(facts "test-create-worker "
       (fact (let [{wid :wid :as w}
                   (rest-post url "worker" tworker)]
               ; got some
               wid => pos?
               ; test get
               (rest-get url "worker" wid) => w
               ; test put for update
               (rest-put url "worker" wid {:wstatus "test status"})
               (rest-get url "worker" wid)
               => (assoc w :wstatus "test status")

               (rest-get url "worker" "ip_pid" (:ip tworker) (:pid tworker))
               => (assoc w :wstatus "test status")

               ; test set admin command
               (-> (rest-put url "worker" wid "admin" {:cmd "SHUTDOWN"})
                   (:admin_cmd))
               => "SHUTDOWN"

               ;test delete
               (rest-delete url "worker" wid)
               (-> (rest-get url "worker" wid)
                   (:wid)) => nil
               )))
