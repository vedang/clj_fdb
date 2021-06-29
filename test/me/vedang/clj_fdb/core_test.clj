(ns me.vedang.clj-fdb.core-test
  (:require
    [byte-streams :as bs]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [me.vedang.clj-fdb.FDB :as cfdb]
    [me.vedang.clj-fdb.core :as fc]
    [me.vedang.clj-fdb.internal.util :as u]
    [me.vedang.clj-fdb.range :as frange]
    [me.vedang.clj-fdb.subspace.subspace :as fsub]
    [me.vedang.clj-fdb.transaction :as ftr]
    [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import
    (com.apple.foundationdb
      Database
      Transaction)))


(use-fixtures :each u/test-fixture)


(deftest get-set-tests
  (testing "Test the best-case path for `fc/set` and `fc/get`"
    (let [k (ftup/from u/*test-prefix* "foo")
          v "1"
          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db k (bs/to-byte-array v))
        (is (= v (fc/get db k bs/to-string)))))))


(deftest get-non-existent-key-tests
  (testing "Test that `fc/get` on a non-existent key returns `nil`"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          k (ftup/from u/*test-prefix* "non-existent")]
      (with-open [^Database db (cfdb/open fdb)]
        (is (nil? (fc/get db k identity)))))))


(deftest clear-key-tests
  (testing "Test the best-case path for `fc/clear`"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          k (ftup/from u/*test-prefix* "foo")
          v "1"]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db k (bs/to-byte-array v))
        (is (= v (fc/get db k bs/to-string)))
        (fc/clear db k)
        (is (nil? (fc/get db k identity)))))))


(deftest get-range-tests
  (testing "Test the best-case path for `fc/get-range`. End is exclusive."
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          input-keys ["bar" "car" "foo" "gum"]
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
               (fc/get-range db
                             rg
                             (comp second ftup/get-items ftup/from-bytes)
                             bs/to-string)))))))


(deftest clear-range-tests
  (testing "Test the best-case path for `fc/clear-range`. End is exclusive."
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          input-keys ["bar" "car" "foo" "gum"]
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

        (is (= v (fc/get db
                         (ftup/from u/*test-prefix* excluded-k)
                         bs/to-string)))))))


(deftest get-set-subspaced-key-tests
  (testing "Get/Set Subspaced Key using empty Tuple"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db prefixed-subspace (ftup/from) "value")
        (is (= "value"
               (fc/get db prefixed-subspace (ftup/from) bs/to-string)))
        (fc/set db prefixed-subspace (ftup/from) "New value")
        (is (= "New value"
               (fc/get db prefixed-subspace (ftup/from) bs/to-string))))))

  (testing "Get/Set Subspaced Key using non-empty Tuple"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db prefixed-subspace (ftup/from "a") "value")
        (= "value"
           (fc/get db prefixed-subspace (ftup/from "a") bs/to-string)))))
  (testing "Get non-existent Subspaced Key"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (is (nil? (fc/get db prefixed-subspace (ftup/from "a") bs/to-string)))))))


(deftest clear-subspaced-key-tests
  (testing "Clear subspaced key"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)]
      (with-open [^Database db (cfdb/open fdb)]
        (fc/set db prefixed-subspace (ftup/from) "value")
        (is (= "value" (fc/get db prefixed-subspace (ftup/from) bs/to-string)))
        (fc/clear db prefixed-subspace (ftup/from))
        (is (nil? (fc/get db prefixed-subspace (ftup/from) bs/to-string)))))))


(deftest get-subspaced-range-tests
  (testing "Get subspaced range"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)
          input-keys ["bar" "car" "foo" "gum"]
          v "10"
          expected-map {"bar" v "car" v "foo" v "gum" v}]
      (with-open [^Database db (cfdb/open fdb)]
        (doseq [k input-keys]
          (fc/set db prefixed-subspace (ftup/from k) v))
        (is (= expected-map
               (fc/get-range db
                             prefixed-subspace
                             (ftup/from)
                             (comp last ftup/get-items ftup/from-bytes)
                             bs/to-string)))))))


(deftest clear-subspaced-range-tests
  (testing "Clear subspaced range completely"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
          prefixed-subspace (fsub/create random-prefixed-tuple)
          input-keys ["bar" "car" "foo" "gum"]
          v "10"
          expected-map {"bar" v "car" v "foo" v "gum" v}]
      (with-open [^Database db (cfdb/open fdb)]
        (doseq [k input-keys]
          (fc/set db prefixed-subspace (ftup/from k) v))
        (is (= expected-map
               (fc/get-range db
                             prefixed-subspace
                             (ftup/from)
                             (comp last ftup/get-items ftup/from-bytes)
                             bs/to-string)))
        (fc/clear-range db prefixed-subspace)
        (is (nil? (fc/get db prefixed-subspace (ftup/from) bs/to-string))))))

  (testing "Clear subspaced range partially"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          random-prefixed-tuple (ftup/from u/*test-prefix* "subspace" (u/rand-str 5))
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
                             (comp ftup/get-items (partial fsub/unpack prefixed-subspace))
                             bs/to-string)))
        (fc/clear-range db prefixed-subspace (ftup/from (first input-keys)))
        (is (= expected-map-2
               (fc/get-range db
                             prefixed-subspace
                             (ftup/from)
                             (comp ftup/get-items (partial fsub/unpack prefixed-subspace))
                             bs/to-string)))))))
