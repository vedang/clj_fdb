(ns me.vedang.clj-fdb.tuple-io-test
  (:require
   [me.vedang.clj-fdb.tuple.converters :as converter]   
   [clojure.test :refer :all]
   [me.vedang.clj-fdb.FDB :as cfdb]
   [me.vedang.clj-fdb.tuple-io :as fio]
   [me.vedang.clj-fdb.internal.util :as u]
   [me.vedang.clj-fdb.range :as frange]
   [me.vedang.clj-fdb.subspace.subspace :as fsubspace]
   [me.vedang.clj-fdb.transaction :as ftr]
   [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb   Database  Transaction]))


(use-fixtures :each u/test-fixture)


(deftest test-set-get-int
  (testing "Test the best-case path for `fio/set` and `fio/get`"
    (let [k-t (ftup/from u/*test-prefix* "foox")
          value-to-store (int 32)
          
          v-t (converter/encode-int value-to-store)
          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          _ (with-open [^Database db (cfdb/open fdb)]
              (fio/set db k-t v-t))
          
          bval (with-open [^Database db (cfdb/open fdb)](fio/get db k-t))
          
          v2 (converter/decode-int bval)
                    ]
        (is (= v2 value-to-store))
      ) ) )
        

(deftest test-set-get-string
  (testing "Test the best-case path for `fio/set` and `fio/get`"
    (let [k-t (ftup/from u/*test-prefix* "foox2")
          value-to-store "Helloo I am a string"

          v-t (.getBytes value-to-store)
          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          _ (with-open [^Database db (cfdb/open fdb)]
              (fio/set db k-t v-t))
          
          bval (with-open [^Database db (cfdb/open fdb)] (fio/get db k-t))
          
          v2 (String. ^bytes bval)
        ]
      (is (= v2 value-to-store)))))


        
