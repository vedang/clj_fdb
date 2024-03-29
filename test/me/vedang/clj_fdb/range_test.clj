(ns me.vedang.clj-fdb.range-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is use-fixtures]]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.internal.util :as u]
            [me.vedang.clj-fdb.range :as frange]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb Database Transaction]))

(use-fixtures :each u/test-fixture)


(deftest range-constructor-tests
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
      (is (= expected-map-1
             (fc/get-range db
                           ;; one style of packing
                           (frange/range (ftup/pack (ftup/from u/*test-prefix* "bar"))
                                         (ftup/pack (ftup/from u/*test-prefix* "baz")))
                           {:keyfn second :valfn bs/to-string})))
      (is (= expected-map-2
             (fc/get-range db
                           ;; another style of packing
                           (frange/range (fimpl/encode [u/*test-prefix* "a"])
                                         (fimpl/encode [u/*test-prefix* "z"]))
                           {:keyfn second :valfn bs/to-string})))
      (is (= {}
             (fc/get-range db
                           (frange/range (ftup/pack (ftup/from u/*test-prefix* "c"))
                                         (ftup/pack (ftup/from u/*test-prefix* "z")))))))))


(deftest range-starts-with-tests
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
      (is (= expected-map-1
             (fc/get-range db
                           ;; one style of packing
                           (frange/starts-with (ftup/pack (ftup/from u/*test-prefix* "bar")))
                           {:keyfn (partial drop 1) :valfn bs/to-string})))
      ;; startswith in tuples requires exact match
      (is (= {}
             (fc/get-range db
                           ;; another style of packing
                           (frange/starts-with (fimpl/encode [u/*test-prefix* "bb"])))))
      (is (= expected-map-2
             (fc/get-range db
                           (frange/starts-with (fimpl/encode [u/*test-prefix* "bbz"]))
                           {:keyfn (partial drop 1) :valfn bs/to-string}))))))
