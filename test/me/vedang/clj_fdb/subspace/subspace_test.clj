(ns me.vedang.clj-fdb.subspace.subspace-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is]]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.internal.util :as u]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import com.apple.foundationdb.Database))

(deftest subspace-get-set-tests
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        random-prefix (str "prefixed-subspace-test:" (u/rand-str 5))
        prefix-subspace (fsub/create (ftup/from random-prefix))
        k-ba (fsub/pack prefix-subspace (ftup/from "test-key"))]
    (with-open [^Database db (cfdb/open fdb)]
      (fc/set db k-ba "subspace value")
      (is (= "subspace value" (fc/get db k-ba {:parsefn bs/to-string}))))))


(deftest subspace-tests
  (let [random-prefix (str "prefixed-subspace-test:" (u/rand-str 5))
        prefix-subspace (fsub/create (ftup/from random-prefix))
        k-ba (fsub/pack prefix-subspace (ftup/from "test-key"))]
    (is (fsub/contains? prefix-subspace k-ba))
    (is (= (fsub/unpack prefix-subspace (fsub/pack prefix-subspace (ftup/from)))
           (ftup/from)))
    (is (= (ftup/from "test")
           (fsub/unpack prefix-subspace
                        (fsub/pack prefix-subspace (ftup/from "test")))))
    (is (= [random-prefix "test"]
           (->> (ftup/from "test")
               (fsub/pack prefix-subspace)
               ftup/from-bytes
               ftup/get-items)))))
