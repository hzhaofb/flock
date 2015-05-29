(ns base.rest-util
  (:require [ring.util.response :as resp]
            [clojure.stacktrace :as st]
            [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [base.util :refer :all]))

(defn echo [req]
  (-> req
      (dissoc :async-channel)
      (assoc :body (slurp (:body req)))))



(defn json-response
  "Takes a function and a list of params,
   apply the function and wraps result to ring response."
  [req f & args]
  (log/debug "Got request " req)
  (try
    (-> (if-let [result (apply f args)]
          (if (associative? result)
            (assoc result :status "OK")
            {:result result :status "OK"})
          {:status "Not Found"})
       (resp/response))
    (catch Throwable ex
      (log/error (with-out-str (st/print-stack-trace ex 50)))
      (resp/response {:status "error" :msg (.getMessage ex)}))))

(defn- to-url
  ([v]
    (->> (map str v)
       (string/join "/")))
  ([k ks]
    (to-url (concat [k] ks))))

(defn rest-get
  "http get from REST endpoint url and return body as map."
  ([url]
    (-> (client/get
          url {:content-type :json :as :json})
        (:body)))
  ([k & ks]
    (rest-get (to-url k ks))))

(defn rest-delete
  "http delete at REST endpoint url and return body as map."
  ([url]
    (-> (client/delete
          url {:content-type :json :as :json})
        (:body)))
  ([k & ks]
    (rest-delete (to-url k ks))))

(defn rest-post
  "http POST a map to REST endpoint url and return body as map.
   If multiple params are passed, they are joined with / in url
   If a single map is passed in url, the map is posted as json string."
  ([url m]
    (-> (client/post
          url {:content-type :json :as :json :form-params m})
        (:body)))
  ([k k1 & ks]
    (rest-post (to-url (concat [k k1] (butlast ks))) (last ks))))

(defn rest-put
  "http PUT a map to REST endpoint url and return body as map.
   If multiple params are passed, they are joined with / in url
   If a single map is passed in url, the map is posted as json string."
  ([url m]
    (-> (client/put
          url {:content-type :json :as :json :form-params m})
        (:body)))
  ([k k1 & ks]
    (rest-put (to-url (concat [k k1] (butlast ks))) (last ks))))
