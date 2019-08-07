(defproject me.vedang/clj-fdb "0.1.0"
  :description "A thin Clojure wrapper for the Java API for FoundationDB."
  :url "https://vedang.github.io/clj_fdb/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.foundationdb/fdb-java "6.1.8"]
                 [byte-streams "0.2.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [org.clojure/tools.logging "0.4.1"]]
                   :plugins [[lein-codox "0.10.7"]]}}
  :target-path "target/%s"
  :global-vars {*warn-on-reflection* true}
  :codox {:source-uri "https://github.com/vedang/clj_fdb/blob/master/{filepath}#L{line}"
          :namespaces [#"^examples" #"^clj-fdb\.(?!internal)"]
          :doc-files ["README.md"]
          :metadata {:doc/format :markdown}})
