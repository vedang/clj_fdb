(ns clj-fdb.core-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-fdb.FDB :as cfdb]
            [clj-fdb.core :as fc]
            [clj-fdb.transaction :as ftr]
            [clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb Range]))

(defn- clear-all
  []
  (let [fdb (cfdb/select-api-version 510)
        begin (byte-array [])
        end (byte-array [0xFF])
        rg (Range. begin end)]
    (with-open [db (cfdb/open fdb)]
      (fc/clear-range db rg))))

(defn- test-fixture
  [test]
  (test)
  (clear-all))

(use-fixtures :each test-fixture)

(deftest test-get-set
  (testing "Test the best-case path for `fc/set` and `fc/get`"
    (let [k (ftup/from "foo")
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
          k "non-existent"]
      (with-open [db (cfdb/open fdb)]
        (is (nil? (fc/get db (ftup/from k))))))))

(deftest test-clear-key
  (testing "Test the best-case path for `fc/clear`"
    (let [fdb (cfdb/select-api-version 510)
          k (ftup/from "foo")
          v (int 1)]
      (with-open [db (cfdb/open fdb)]
        (fc/set db k v)
        (is (= (fc/get db k :valfn #(bs/convert %1 Integer))
               v))
        (fc/clear db k)
        (is (nil? (fc/get db k)))))))

(deftest test-get-range
  (testing "Test the best-case path for `fc/get-range`. End is exclusive."
    (let [input-keys ["bar" "car" "foo" "gum"]
          begin      (.pack (ftup/from "b"))
          end        (.pack (ftup/from "g"))
          rg         (Range. begin end)
          v          (int 1)
          expected   {"bar" v "car" v "foo" v}]
      (let [fdb (cfdb/select-api-version 510)]
        (with-open [db (cfdb/open fdb)]
          (doseq [k input-keys]
            (let [k (ftup/from k)]
              (fc/set db k v)))

          (is (= (fc/get-range db rg
                               :keyfn (comp first ftup/get-items ftup/from-bytes)
                               :valfn #(bs/convert %1 Integer))
                 expected)))))))

(deftest test-clear-range
  (testing "Test the best-case path for `fc/clear-range`. End is exclusive."
    (let [input-keys ["bar" "car" "foo" "gum"]
          begin      (.pack (ftup/from "b"))
          end        (.pack (ftup/from "g"))
          rg         (Range. begin end)
          v          (int 1)
          k          (ftup/from "gum")]
      (let [fdb (cfdb/select-api-version 510)]
        (with-open [db (cfdb/open fdb)]
          (doseq [k input-keys]
            (let [k (ftup/from k)]
              (fc/set db k v)))

          (fc/clear-range db rg)
          (is (= (fc/get db k :valfn #(bs/convert %1 Integer)) v)))))))
