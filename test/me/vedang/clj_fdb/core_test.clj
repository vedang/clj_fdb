(ns me.vedang.clj-fdb.core-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.directory.directory :as fdir]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.internal.util :as u]
            [me.vedang.clj-fdb.range :as frange]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb Database Transaction]
           java.nio.ByteBuffer))

(use-fixtures :each u/test-fixture)

(def fdb (cfdb/select-api-version cfdb/clj-fdb-api-version))

(deftest get-set-tests
  (testing "Test the best-case path for `fc/set` and `fc/get`"
    (let [k (ftup/from u/*test-prefix* "foo")
          v "1"]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db k (bs/to-byte-array v))
        (is (= v (fc/get db k {:valfn bs/to-string})))))))


(deftest get-non-existent-key-tests
  (testing "Test that `fc/get` on a non-existent key returns `nil`"
    (let [k (ftup/from u/*test-prefix* "non-existent")]
      (with-open [^Database db (cfdb/open fdb)]
        (is (nil? (fc/get db k)))))))


(deftest clear-key-tests
  (testing "Test the best-case path for `fc/clear`"
    (let [k (ftup/from u/*test-prefix* "foo")
          v "1"]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db k (bs/to-byte-array v))
        (is (= v (fc/get db k {:valfn bs/to-string})))
        (fc/clear db k)
        (is (nil? (fc/get db k)))))))


(deftest get-range-tests
  (testing "Test the best-case path for `fc/get-range`. End is exclusive."
    (let [input-keys ["bar" "car" "foo" "gum"]
          begin      (ftup/pack (ftup/from u/*test-prefix* "b"))
          end        (ftup/pack (ftup/from u/*test-prefix* "g"))
          rg         (frange/range begin end)
          v          "1"
          expected-map {"bar" v "car" v "foo" v}]
      (with-open [^Database db (cfdb/open fdb)]
        (ftr/run db
          (fn [^Transaction tr]
            (doseq [k input-keys]
              (let [k (ftup/from u/*test-prefix* k)]
                (fc/set tr k (bs/to-byte-array v))))))

        (is (= expected-map
               (fc/get-range db rg {:keyfn second :valfn bs/to-string})))))))

(deftest get-range-accumulator-test
  (testing "Test the best-case path for `fc/get-range`. End is exclusive."
    (let [input-keys ["bar" "car" "foo" "gum"]
          begin      (ftup/pack (ftup/from u/*test-prefix* "b"))
          end        (ftup/pack (ftup/from u/*test-prefix* "g"))
          rg         (frange/range begin end)
          v          "1"
          expected-vec [["bar" v] ["car" v] ["foo" v]]]
      (with-open [^Database db (cfdb/open fdb)]
        (ftr/run db
          (fn [^Transaction tr]
            (doseq [k input-keys]
              (let [k (ftup/from u/*test-prefix* k)]
                (fc/set tr k (bs/to-byte-array v))))))

        (is (= expected-vec
               (fc/get-range db rg {:keyfn second :valfn bs/to-string :coll []})))))))


(deftest clear-range-tests
  (testing "Test the best-case path for `fc/clear-range`. End is exclusive."
    (let [input-keys ["bar" "car" "foo" "gum"]
          begin      (ftup/pack (ftup/from u/*test-prefix* "b"))
          end        (ftup/pack (ftup/from u/*test-prefix* "g"))
          rg         (frange/range begin end)
          v          "1"
          excluded-k "gum"]
      (with-open [^Database db (cfdb/open fdb)]
        (ftr/run db
          (fn [^Transaction tr]
            (doseq [k input-keys]
              (let [k (ftup/from u/*test-prefix* k)]
                (fc/set tr k (bs/to-byte-array v))))))
        (fc/clear-range db rg)

        (is (= v (fc/get db (ftup/from u/*test-prefix* excluded-k)
                         {:valfn bs/to-string})))))))


(deftest get-set-subspaced-key-tests
  (testing "Get/Set Subspaced Key using empty Tuple"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db prefixed-subspace (ftup/from) "value")
        (is (= "value"
               (fc/get db prefixed-subspace (ftup/from)
                       {:valfn bs/to-string})))
        (fc/set db prefixed-subspace (ftup/from) "New value")
        (is (= "New value"
               (fc/get db prefixed-subspace (ftup/from)
                       {:valfn bs/to-string}))))))

  (testing "Get/Set Subspaced Key using non-empty Tuple"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db prefixed-subspace (ftup/from "a") "value")
        (is (= "value"
               (fc/get db prefixed-subspace (ftup/from "a")
                       {:valfn bs/to-string}))))))
  (testing "Get non-existent Subspaced Key"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (is (nil? (fc/get db prefixed-subspace (ftup/from "a"))))))))


(deftest clear-subspaced-key-tests
  (testing "Clear subspaced key"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db prefixed-subspace (ftup/from) "value")
        (is (= "value" (fc/get db prefixed-subspace (ftup/from)
                               {:valfn bs/to-string})))
        (fc/clear db prefixed-subspace (ftup/from))
        (is (nil? (fc/get db prefixed-subspace (ftup/from))))))))


(deftest get-subspaced-range-tests
  (testing "Get subspaced range"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)
          input-keys ["bar" "car" "foo" "gum"]
          v "10"
          expected-map {"bar" v "car" v "foo" v "gum" v}]
      (with-open [^Database db (cfdb/open fdb)]
        (doseq [k input-keys]
          (fc/set db prefixed-subspace (ftup/from k) v))
        (is (= expected-map
               (fc/get-range db prefixed-subspace (ftup/from)
                             {:keyfn last :valfn bs/to-string})))))))


(deftest clear-subspaced-range-tests
  (testing "Clear subspaced range completely"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)
          input-keys ["bar" "car" "foo" "gum"]
          v "10"
          expected-map {"bar" v "car" v "foo" v "gum" v}]
      (with-open [^Database db (cfdb/open fdb)]
        (doseq [k input-keys]
          (fc/set db prefixed-subspace (ftup/from k) v))
        (is (= expected-map
               (fc/get-range db prefixed-subspace (ftup/from)
                             {:keyfn last :valfn bs/to-string})))
        (fc/clear-range db prefixed-subspace)
        (is (nil? (fc/get db prefixed-subspace (ftup/from)))))))

  (testing "Clear subspaced range partially"
    (let [random-prefixed-tuple (ftup/from u/*test-prefix*
                                           "subspace"
                                           (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)
          input-keys ["bar" ["bar" "bar"] ["bar" "baz"] "car" "foo" "gum"]
          v "10"
          expected-map {["bar"] v ["car"] v ["foo"] v ["gum"] v ["bar" "bar"] v ["bar" "baz"] v}
          expected-map-2 {["bar"] v ["car"] v ["foo"] v ["gum"] v}]
      (with-open [^Database db (cfdb/open fdb)]
        (doseq [k input-keys]
          (fc/set db
                  prefixed-subspace
                  (if (sequential? k)
                    (apply ftup/from k)
                    (ftup/from k))
                  v))
        (is (= expected-map
               (fc/get-range db
                             prefixed-subspace
                             (ftup/from)
                             {:valfn bs/to-string})))
        (fc/clear-range db prefixed-subspace (ftup/from (first input-keys)))
        (is (= expected-map-2
               (fc/get-range db
                             prefixed-subspace
                             (ftup/from)
                             {:valfn bs/to-string})))))))


(deftest get-set-directory-key-tests
  (let [random-prefixed-path [u/*test-prefix* "subspace" (u/rand-str 5)]]
    (testing "Get/Set Key inside a directory using empty Tuple"
      (with-open [^Database db (cfdb/open fdb)]
        (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
          (fc/set db test-dir (ftup/from) "value")
          (is (= "value" (fc/get db test-dir (ftup/from)
                                 {:valfn bs/to-string})))
          (fc/set db test-dir (ftup/from) "New value")
          (is (= "New value" (fc/get db test-dir (ftup/from)
                                     {:valfn bs/to-string}))))))

    (testing "Get/Set Key inside a directory using non-empty Tuple"
      (with-open [^Database db (cfdb/open fdb)]
        (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
          (fc/set db test-dir (ftup/from "a") "value")
          (is (= "value" (fc/get db test-dir (ftup/from "a")
                                 {:valfn bs/to-string}))))))

    (testing "Get non-existent Key in directory"
      (with-open [^Database db (cfdb/open fdb)]
        (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
          (is (nil? (fc/get db test-dir (ftup/from "aba")))))))))


(deftest clear-directory-key-tests
  (let [random-prefixed-path [u/*test-prefix* "subspace" (u/rand-str 5)]]
    (testing "Clear a key that is nested in a directory"
      (with-open [^Database db (cfdb/open fdb)]
        (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
          (fc/set db test-dir (ftup/from) "value")
          (is (= "value" (fc/get db test-dir (ftup/from)
                                 {:valfn bs/to-string})))
          (fc/clear db test-dir (ftup/from))
          (is (nil? (fc/get db test-dir (ftup/from)))))))))


(deftest get-directory-range-tests
  (let [random-prefixed-path [u/*test-prefix* "subspace" (u/rand-str 5)]
        input-keys ["bar" "car" "foo" "gum"]
        v ["10"]
        expected-map {["bar"] v ["car"] v ["foo"] v ["gum"] v}]
    (testing "Get directory range of data"
      (with-open [^Database db (cfdb/open fdb)]
        (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
          (doseq [k input-keys]
            (fc/set db test-dir (ftup/from k) v))
          (is (= expected-map (fc/get-range db test-dir))))))))


(deftest clear-directory-range-tests
  (let [random-prefixed-path [u/*test-prefix* "subspace" (u/rand-str 5)]
        input-keys ["bar" "car" "foo" "gum"]
        v "10"
        expected-map {"bar" v "car" v "foo" v "gum" v}]
    (testing "Clear keys under directory range completely"
      (with-open [^Database db (cfdb/open fdb)]
        (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
          (doseq [k input-keys]
            (fc/set db test-dir (ftup/from k) v))
          (is (= expected-map
                 (fc/get-range db test-dir (ftup/from)
                               {:keyfn last :valfn bs/to-string})))
          (fc/clear-range db test-dir)
          (is (nil? (fc/get db test-dir (ftup/from)))))))

    (testing "Clear subspaced range partially"
      (let [input-keys ["bar" ["bar" "bar"] ["bar" "baz"] "car" "foo" "gum"]
            v "10"
            expected-map {["bar"] v ["car"] v ["foo"] v ["gum"] v ["bar" "bar"] v ["bar" "baz"] v}
            expected-map-2 {["bar"] v ["car"] v ["foo"] v ["gum"] v}]
        (with-open [^Database db (cfdb/open fdb)]
          (let [test-dir (fdir/create-or-open! db random-prefixed-path)]
            (doseq [k input-keys]
              (fc/set db
                      test-dir
                      (if (sequential? k)
                        (apply ftup/from k)
                        (ftup/from k))
                      v))
            (is (= expected-map
                   (fc/get-range db
                                 test-dir
                                 (ftup/from)
                                 {:valfn bs/to-string})))
            (fc/clear-range db test-dir (ftup/from (first input-keys)))
            (is (= expected-map-2
                   (fc/get-range db
                                 test-dir
                                 (ftup/from)
                                 {:valfn bs/to-string})))))))))

(deftest vector-as-input-tests
  (let [random-prefixed-path [u/*test-prefix* (u/rand-str 5)]
        random-key ["random-key"]
        random-spc (fsub/create random-prefixed-path)]
    (testing "Get/Setting keys with vectors as Tuples"
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db random-prefixed-path [])
        (is (= []
               (fc/get db (apply ftup/from random-prefixed-path))
               (fc/get db random-prefixed-path)))))
    (testing "Get/Setting keys with vectors as Subspaces + Tuples"
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db random-prefixed-path random-key [])
        (is (= []
               (fc/get db (fsub/pack (fsub/create (apply ftup/from random-prefixed-path))
                                     (apply ftup/from random-key)))
               (fc/get db random-prefixed-path random-key)
               (fc/get db random-spc random-key)
               (fc/get db random-prefixed-path
                       (apply ftup/from random-key))))))))


(def byte-array-class (class (byte-array 0)))

(bs/def-conversion [java.lang.Integer byte-array-class]
  [integer]
  (.. (ByteBuffer/allocate 4)
      (putInt integer)
      array))

(bs/def-conversion [byte-array-class java.lang.Integer]
  [integer-ba]
  (.. (ByteBuffer/wrap integer-ba)
      (getInt)))


(deftest mutation-tests
  (let [random-prefixed-path [u/*test-prefix* (u/rand-str 5)]
        random-key ["random-key"]
        random-spc (fsub/create random-prefixed-path)]
    (testing "Mutation with no subspace passed"
      (with-open [^Database db (cfdb/open fdb)]
        (fc/mutate! db :add random-key)
        (is (= [] (fc/get db random-key)))
        (fc/mutate! db :add random-key (bs/to-byte-array (int 1)))
        (is (= 1 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :add random-key (bs/to-byte-array (int 1)))
        (is (= 2 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-or random-key (bs/to-byte-array (int 1)))
        (is (= 3 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-and random-key (bs/to-byte-array (int 1)))
        (is (= 1 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-xor random-key (bs/to-byte-array (int 2)))
        (is (= 3 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-xor random-key (bs/to-byte-array (int 1)))
        (is (= 2 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :max random-key (bs/to-byte-array (int 1)))
        (is (= 2 (fc/get db random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :max random-key (bs/to-byte-array (int 3)))
        (is (= 3 (fc/get db random-key {:valfn #(bs/convert % Integer)})))))
    (testing "Mutation with subspace passed"
      (with-open [^Database db (cfdb/open fdb)]
        (fc/mutate! db :add random-spc random-key)
        (is (= [] (fc/get db random-spc random-key)))
        (fc/mutate! db :add random-spc random-key (bs/to-byte-array (int 1)))
        (is (= 1 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :add random-spc random-key (bs/to-byte-array (int 1)))
        (is (= 2 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-or random-spc random-key (bs/to-byte-array (int 1)))
        (is (= 3 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-and random-spc random-key (bs/to-byte-array (int 1)))
        (is (= 1 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-xor random-spc random-key (bs/to-byte-array (int 2)))
        (is (= 3 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :bit-xor random-spc random-key (bs/to-byte-array (int 1)))
        (is (= 2 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :max random-spc random-key (bs/to-byte-array (int 1)))
        (is (= 2 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))
        (fc/mutate! db :max random-spc random-key (bs/to-byte-array (int 3)))
        (is (= 3 (fc/get db random-spc random-key {:valfn #(bs/convert % Integer)})))))))
