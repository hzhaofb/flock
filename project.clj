(defproject flock "1.0.0"
  :description "Flock task schedule and distribution service"
  :url "http://com/"
  :license {:name ""
            :url "http://com/copyright"}
  :dependencies [
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [mysql/mysql-connector-java "5.1.34"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clojurewerkz/propertied "1.2.0"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [cheshire "5.4.0"]
                 [clj-http "1.0.1"]
                 [clj-time "0.9.0"]
                 [com.stuartsierra/component "0.2.2"]
                 [compojure "1.3.1"]
                 [http-kit "2.1.19"]
                 [ring/ring-core "1.3.2"]
                 [ring/ring-json "0.3.1"]]
  :jvm-opts ["-Xmx2g" "-server"]
  :main ^:skip-aot flock.main
  :target-path "target/%s"
  :uberjar-name "flock-standalone.jar"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.clojure/tools.namespace "0.2.7"]
                                  [midje "1.6.3"]
                                  [reloaded.repl "0.1.0"]
                                  ]
                   :repl-options {:init-ns user}
                   :source-paths ["dev"]
                   :test-paths ["test" "test-int"]
                   :plugins [[lein-midje "3.1.1"]]}})
