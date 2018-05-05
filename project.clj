(defproject clj-fdb "0.1.0"
  :description "A thin Clojure wrapper for the Java API for FoundationDB."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.apple.foundationdb/fdb-java "5.1.7"]
                 [byte-streams "0.2.3"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
