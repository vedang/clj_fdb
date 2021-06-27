(ns me.vedang.clj-fdb.subspace-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is]]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.internal.util :as u]
            [me.vedang.clj-fdb.subspace.subspace :as fss]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import com.apple.foundationdb.Database))

(deftest test-prefixed-subspace
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        random-prefix (str "prefixed-subspace-test:" (u/rand-str 5))
        prefix-subspace (fss/create-subspace (ftup/from random-prefix))
        k-ba (fss/pack prefix-subspace (ftup/from "test-key"))]
    (with-open [^Database db (cfdb/open fdb)]
      (fc/set db k-ba "subspace value")
      (is (= "subspace value" (fc/get db k-ba bs/to-string)))
      (is (fss/contains? prefix-subspace k-ba))
      (is (= (fss/unpack prefix-subspace (fss/pack prefix-subspace (ftup/from)))
             (ftup/from)))
      (is (= (ftup/from "test")
             (fss/unpack prefix-subspace
                         (fss/pack prefix-subspace (ftup/from "test"))))))))
