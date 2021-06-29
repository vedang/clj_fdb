(ns me.vedang.clj-fdb.directory.directory-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [me.vedang.clj-fdb.directory.directory :as fdir]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.internal.util :as u])
  (:import com.apple.foundationdb.Database))

(def fdb (cfdb/select-api-version cfdb/clj-fdb-api-version))

(use-fixtures :each u/test-fixture)

(deftest directory-tests
  (let [random-prefix [u/*test-prefix*]
        prefix-path-1 [u/*test-prefix* "test-path-1"]
        prefix-path-2 [u/*test-prefix* "test-path-2"]]
    (with-open [^Database db (cfdb/open fdb)]
      (let [ds (fdir/create-or-open! db random-prefix)]
        (is (fdir/exists? db random-prefix))
        (is (empty? (fdir/list db ds)))
        (is (not (fdir/exists? db prefix-path-1)))
        (fdir/create-or-open! db prefix-path-1)
        (is (fdir/exists? db prefix-path-1))
        (fdir/create-or-open! db prefix-path-2)
        (is (fdir/exists? db prefix-path-2))
        (is (= (set '("test-path-1" "test-path-2")) (set (fdir/list db ds))))
        (deref (fdir/remove! db prefix-path-1))
        (is (not (fdir/exists? db prefix-path-1)))
        (is (fdir/exists? db prefix-path-2))
        (deref (fdir/remove! db prefix-path-2))
        (is (not (fdir/exists? db prefix-path-2)))))))
