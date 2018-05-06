(ns clj-fdb.core-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-fdb.FDB :as cfdb]
            [clj-fdb.core :as fc]
            [clj-fdb.transaction :as ftr]
            [clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb Range]))

(defn clear-all
  []
  (let [fdb (cfdb/select-api-version 510)]
    (with-open [db (cfdb/open fdb)]
      (ftr/run db (fn [tr]
                    (let [begin (byte-array [])
                          end   (byte-array [0xFF])
                          rg    (Range. begin end)]
                      (fc/clear-range tr rg)))))))

(defn test-fixture
  [test]
  (test)
  (clear-all))

(use-fixtures :each test-fixture)


(deftest test-set
  (testing "Test simple set"
    (let [key   (ftup/from "foo")
          value (int 1)]
      (let [fdb (cfdb/select-api-version 510)]
        (with-open [db (cfdb/open fdb)]
          (ftr/run db (fn [tr] (fc/set tr key value))))
        (with-open [db (cfdb/open fdb)]
          (is (= (ftr/run db (fn [tr]
                               (fc/get tr key
                                       :valfn #(bs/convert %1 Integer)))))
              value))))))

(deftest test-non-existent
  (testing "Test non-existent key to return nil"
    (let [fdb (cfdb/select-api-version 510)
          key "non-existent"]
      (with-open [db (cfdb/open fdb)]
        (is (nil? (ftr/run db (fn [tr] (fc/get tr (ftup/from key))))))))))

(deftest test-clear-key
  (testing "Test clear key"
    (let [fdb   (cfdb/select-api-version 510)
          key   (ftup/from "foo")
          value (int 1)]
      (with-open [db (cfdb/open fdb)]
        (ftr/run db (fn [tr] (fc/set tr key value)))
        (ftr/run db (fn [tr] (fc/clear tr key)))
        (is (nil? (ftr/run db (fn [tr] (fc/get tr key)))))))))
