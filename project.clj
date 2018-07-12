(defproject clj-fdb "0.1.0"
  :description "A thin Clojure wrapper for the Java API for FoundationDB."
  :url "https://vedang.github.io/clj_fdb/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.foundationdb/fdb-java "6.0.15"]
                 [byte-streams "0.2.3"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.9.0"]
                                  [org.clojure/tools.logging "0.4.1"]]
                   :plugins [[lein-codox "0.10.4"]]}}
  :target-path "target/%s"
  :codox {:source-uri "https://github.com/vedang/clj_fdb/blob/master/{filepath}#L{line}"
          :namespaces [#"^examples" #"^clj-fdb\.(?!internal)"]
          :doc-files ["README.md"]
          :metadata {:doc/format :markdown}})
