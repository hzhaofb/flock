(defproject flock "1.0.0"
  :description "Flock task schedule and distribution service"
  :url "http://com/"
  :license {:name ""
            :url "http://com/copyright"}
  :dependencies [
                 [c3p0/c3p0 "0.9.1.2"]
                 [cheshire "5.4.0"]
                 [clj-http "1.0.1"]
                 [clj-time "0.9.0"]
                 [clojurewerkz/propertied "1.2.0"]
                 [clojurewerkz/spyglass "1.1.0"]
                 [com.palletops/thread-expr "1.3.0"]
                 [com.stuartsierra/component "0.2.2"]
                 [compojure "1.3.1"]
                 [de.ubercode.clostache/clostache "1.4.0"]
                 [http-kit "2.1.19"]
                 [metrics-clojure "2.4.0"]
                 [metrics-clojure-graphite "2.4.0"]
                 [metrics-clojure-ring "2.4.0"]
                 [mysql/mysql-connector-java "5.1.34"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/test.check "0.6.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.nrepl "0.2.8"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]
                 [ring/ring-core "1.3.2"]
                 [ring/ring-json "0.3.1"]
                 [stencil "0.3.5"]
                 ]
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
