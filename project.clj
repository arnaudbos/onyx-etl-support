(defproject onyx-etl-support "0.8.2.0-SNAPSHOT"
  :description "Supporting code for the onyx-etl tool"
  :url "https://github.com/onyx-platform/onyx-etl-support"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.onyxplatform/onyx "0.8.2"]
                 [org.onyxplatform/onyx-datomic "0.8.2.4"]
                 [org.onyxplatform/onyx-sql "0.8.2.1"]
                 [com.datomic/datomic-free "0.9.5327" :exclusions [joda-time]]])
