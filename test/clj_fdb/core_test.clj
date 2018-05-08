(ns clj-fdb.core-test
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

(deftest test-get-set
  (testing "Test the best-case path for `fc/set` and `fc/get`"
    (let [k (ftup/from *prefix* "foo")
          v (int 1)]
      (let [fdb (cfdb/select-api-version 510)]
        (with-open [db (cfdb/open fdb)]
          (fc/set db k v))
        (with-open [db (cfdb/open fdb)]
          (is (= (fc/get db k :valfn #(bs/convert %1 Integer))
                 v)))))))

(deftest test-get-non-existent-key
  (testing "Test that `fc/get` on a non-existent key returns `nil`"
    (let [fdb (cfdb/select-api-version 510)
          k (ftup/from *prefix* "non-existent")]
      (with-open [db (cfdb/open fdb)]
        (is (nil? (fc/get db k)))))))

(deftest test-clear-key
  (testing "Test the best-case path for `fc/clear`"
    (let [fdb (cfdb/select-api-version 510)
          k (ftup/from *prefix* "foo")
          v (int 1)]
      (with-open [db (cfdb/open fdb)]
        (fc/set db k v)
        (is (= (fc/get db k :valfn #(bs/convert %1 Integer))
               v))
        (fc/clear db k)
        (is (nil? (fc/get db k)))))))

(deftest test-get-range
  (testing "Test the best-case path for `fc/get-range`. End is exclusive."
    (let [fdb (cfdb/select-api-version 510)
          input-keys ["bar" "car" "foo" "gum"]
          begin      (ftup/pack (ftup/from *prefix* "b"))
          end        (ftup/pack (ftup/from *prefix* "g"))
          rg         (frange/range begin end)
          v          (int 1)
          expected-map {"bar" v "car" v "foo" v}]
      (with-open [db (cfdb/open fdb)]
        (ftr/run db
          (fn [^Transaction tr]
            (doseq [k input-keys]
              (let [k (ftup/from *prefix* k)]
                (fc/set tr k v)))))

        (is (= (fc/get-range db rg
                             :keyfn (comp second ftup/get-items ftup/from-bytes)
                             :valfn #(bs/convert %1 Integer))
               expected-map))))))

(deftest test-clear-range
  (testing "Test the best-case path for `fc/clear-range`. End is exclusive."
    (let [fdb (cfdb/select-api-version 510)
          input-keys ["bar" "car" "foo" "gum"]
          begin      (ftup/pack (ftup/from *prefix* "b"))
          end        (ftup/pack (ftup/from *prefix* "g"))
          rg         (frange/range begin end)
          v          (int 1)
          excluded-k "gum"]
      (with-open [db (cfdb/open fdb)]
        (ftr/run db
          (fn [^Transaction tr]
            (doseq [k input-keys]
              (let [k (ftup/from *prefix* k)]
                (fc/set tr k v)))))
        (fc/clear-range db rg)

        (is (= (fc/get db (ftup/from *prefix* excluded-k)
                       :valfn #(bs/convert % Integer))
               v))))))
