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
        prefix-subspace (fss/create-subspace (ftup/from random-prefix))]
    (with-open [^Database db (cfdb/open fdb)]
      (fc/set db
              (fss/pack prefix-subspace (ftup/from "test-key"))
              "subspace value")
      (is (= "subspace value"
             (fc/get db
                     (fss/pack prefix-subspace (ftup/from "test-key"))
                     :valfn #(bs/convert % String))))
      (is (fss/contains? prefix-subspace
                         (fss/pack prefix-subspace (ftup/from "test-key"))))
      (is (= (fss/unpack prefix-subspace (fss/pack prefix-subspace (ftup/from)))
             (ftup/from)))
      (is (= (fss/unpack prefix-subspace
                         (fss/pack prefix-subspace (ftup/from "test")))
             (ftup/from "test"))))))
