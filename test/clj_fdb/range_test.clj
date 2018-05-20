(ns clj-fdb.range-test
  (:require [byte-streams :as bs]
            [clj-fdb.core :as fc]
            [clj-fdb.FDB :as cfdb]
            [clj-fdb.internal.util :as u]
            [clj-fdb.range :as frange]
            [clj-fdb.transaction :as ftr]
            [clj-fdb.tuple.tuple :as ftup]
            [clojure.test :refer :all])
  (:import com.apple.foundationdb.Transaction))

(def ^:dynamic *prefix* nil)

(defn- clear-all-with-prefix
  "Helper fn to ensure sanity of DB"
  [prefix]
  (let [fdb (cfdb/select-api-version 510)
        rg (ftup/range (ftup/from prefix))]
    (with-open [db (cfdb/open fdb)]
      (fc/clear-range db rg))))

(defn- test-fixture
  [test]
  (let [random-prefix (str "testcycle:" (u/rand-str 5))]
    (binding [*prefix* random-prefix]
      (test))
    (clear-all-with-prefix random-prefix)))

(use-fixtures :each test-fixture)

(deftest test-range-contructor
  (let [fdb (cfdb/select-api-version 510)
        test-keys ["bar" "bas" "bbt" "baq" "baz"]
        test-val "TESTVAL"
        expected-map-1 (zipmap ["bar" "bas"] (repeat test-val))
        expected-map-2 (zipmap test-keys (repeat test-val))]
    (with-open [db (cfdb/open fdb)]
      (ftr/run db
        (fn [^Transaction tr]
          (doseq [k test-keys]
            (fc/set tr (ftup/from *prefix* k) test-val))))
      (is (= (fc/get-range db
                           (frange/range (ftup/pack (ftup/from *prefix* "bar"))
                                         (ftup/pack (ftup/from *prefix* "baz")))
                           :keyfn (comp second ftup/get-items ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-1))
      (is (= (fc/get-range db
                           (frange/range (ftup/pack (ftup/from *prefix* "a"))
                                         (ftup/pack (ftup/from *prefix* "z")))
                           :keyfn (comp second ftup/get-items ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-2))
      (is (= (fc/get-range db
                           (frange/range (ftup/pack (ftup/from *prefix* "c"))
                                         (ftup/pack (ftup/from *prefix* "z")))
                           :keyfn (comp second ftup/get-items ftup/from-bytes)
                           :valfn #(bs/convert % String))
             {})))))

(deftest test-range-starts-with
  (let [fdb (cfdb/select-api-version 510)
        test-keys [["bar" "a"] ["bar" "ba"]
                   ["bas" "a"] ["bas" "ba"]
                   ["bbt" "a"]
                   ["bbq" "a"]
                   ["bbz" "a"]]
        test-val "TESTVAL"
        expected-map-1 (zipmap [["bar" "a"] ["bar" "ba"]] (repeat test-val))
        expected-map-2 {(last test-keys) test-val}]
    (with-open [db (cfdb/open fdb)]
      (ftr/run db
        (fn [^Transaction tr]
          (doseq [k test-keys]
            (fc/set tr (apply ftup/from *prefix* k) test-val))))
      (is (= (fc/get-range db
                           (frange/starts-with (ftup/pack (ftup/from *prefix* "bar")))
                           :keyfn (comp (partial drop 1)
                                        ftup/get-items
                                        ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-1))
      ;; startswith in tuples requires exact match
      (is (= (fc/get-range db
                           (frange/starts-with (ftup/pack (ftup/from *prefix* "bb")))
                           :keyfn (comp (partial drop 1)
                                        ftup/get-items
                                        ftup/from-bytes)
                           :valfn #(bs/convert % String))
             {}))
      (is (= (fc/get-range db
                           (frange/starts-with (ftup/pack (ftup/from *prefix* "bbz")))
                           :keyfn (comp (partial drop 1)
                                        ftup/get-items
                                        ftup/from-bytes)
                           :valfn #(bs/convert % String))
             expected-map-2)))))
