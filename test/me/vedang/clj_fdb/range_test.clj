(ns me.vedang.clj-fdb.range-test
  (:require
    [byte-streams :as bs]
    [clojure.test :refer :all]
    [me.vedang.clj-fdb.FDB :as cfdb]
    [me.vedang.clj-fdb.core :as fc]
    [me.vedang.clj-fdb.internal.util :as u]
    [me.vedang.clj-fdb.range :as frange]
    [me.vedang.clj-fdb.transaction :as ftr]
    [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import
    (com.apple.foundationdb
      Database
      Transaction)))


(use-fixtures :each u/test-fixture)


(deftest test-range-contructor
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        test-keys ["bar" "bas" "bbt" "baq" "baz"]
        test-val "TESTVAL"
        expected-map-1 (zipmap ["bar" "bas"] (repeat test-val))
        expected-map-2 (zipmap test-keys (repeat test-val))]
    (with-open [^Database db (cfdb/open fdb)]
      (ftr/run db
        (fn [^Transaction tr]
          (doseq [k test-keys]
            (fc/set tr (ftup/from u/*test-prefix* k) test-val))))
      (is (= (fc/get-range db
                           (frange/range (ftup/pack (ftup/from u/*test-prefix* "bar"))
                                         (ftup/pack (ftup/from u/*test-prefix* "baz")))
                           :keyfn (comp second ftup/get-items ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-1))
      (is (= (fc/get-range db
                           (frange/range (ftup/pack (ftup/from u/*test-prefix* "a"))
                                         (ftup/pack (ftup/from u/*test-prefix* "z")))
                           :keyfn (comp second ftup/get-items ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-2))
      (is (= (fc/get-range db
                           (frange/range (ftup/pack (ftup/from u/*test-prefix* "c"))
                                         (ftup/pack (ftup/from u/*test-prefix* "z")))
                           :keyfn (comp second ftup/get-items ftup/from-bytes)
                           :valfn #(bs/convert % String))
             {})))))

(deftest test-range-starts-with
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        test-keys [["bar" "a"] ["bar" "ba"]
                   ["bas" "a"] ["bas" "ba"]
                   ["bbt" "a"]
                   ["bbq" "a"]
                   ["bbz" "a"]]
        test-val "TESTVAL"
        expected-map-1 (zipmap [["bar" "a"] ["bar" "ba"]] (repeat test-val))
        expected-map-2 {(last test-keys) test-val}]
    (with-open [^Database db (cfdb/open fdb)]
      (ftr/run db
        (fn [^Transaction tr]
          (doseq [k test-keys]
            (fc/set tr (apply ftup/from u/*test-prefix* k) test-val))))
      (is (= (fc/get-range db
                           (-> u/*test-prefix*
                               (ftup/from "bar")
                               ftup/pack
                               frange/starts-with)
                           :keyfn (comp (partial drop 1)
                                        ftup/get-items
                                        ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-1))
      ;; startswith in tuples requires exact match
      (is (= (fc/get-range db
                           (-> u/*test-prefix*
                               (ftup/from "bb")
                               ftup/pack
                               frange/starts-with)
                           :keyfn (comp (partial drop 1)
                                        ftup/get-items
                                        ftup/from-bytes)
                           :valfn #(bs/convert % String))
             {}))
      (is (= (fc/get-range db
                           (-> u/*test-prefix*
                               (ftup/from "bbz")
                               ftup/pack
                               frange/starts-with)
                           :keyfn (comp (partial drop 1)
                                        ftup/get-items
                                        ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-2)))))
