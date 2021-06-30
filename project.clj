(defproject me.vedang/clj-fdb "0.3.0-SNAPSHOT"
  :description "A thin Clojure wrapper for the Java API for FoundationDB."
  :url "https://vedang.github.io/clj_fdb/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.foundationdb/fdb-java "6.3.13"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]
                                  [org.clojure/tools.logging "1.1.0"]
                                  [byte-streams "0.2.4"]]
                   :jvm-opts ["-Dclojure.tools.logging.factory=clojure.tools.logging.impl/slf4j-factory"]
                   :plugins [[lein-codox "0.10.7"]]}}
  :target-path "target/%s"
  :global-vars {*warn-on-reflection* true}
  :codox {:source-uri "https://github.com/vedang/clj_fdb/blob/master/{filepath}#L{line}"
          :namespaces [#"^examples" #"^me\.vedang\.clj-fdb\.(?!internal)"]
          :doc-files ["README.md"]
          :metadata {:doc/format :markdown}})
