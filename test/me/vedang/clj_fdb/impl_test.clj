(ns me.vedang.clj-fdb.impl-test
  (:require [clojure.test :refer [deftest is]]
            [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import java.util.UUID))


(deftest encode-decode-tests
  (is (= [42 43 44] (-> [42 43 44] fimpl/encode fimpl/decode)))
  (is (= [] (-> [] fimpl/encode fimpl/decode)))

  (let [id (UUID/randomUUID)]
    (is (= [id] (-> [id] fimpl/encode fimpl/decode))))

  (is (= ["test-subspace" 1 2 3]
         (->> [1 2 3] (fimpl/encode ["test-subspace"]) fimpl/decode)))
  (is (= [1 2 3]
         (->> [1 2 3]
              (fimpl/encode ["test-subspace"])
              (fimpl/decode ["test-subspace"]))
         (->> [1 2 3]
              (fimpl/encode ["test-subspace"])
              (fimpl/decode (fsub/create ["test-subspace"]))))))


(deftest handle-opts-tests
  (is (= {} (last (fimpl/handle-opts (fsub/create) (ftup/create [])))))
  (is (nil? (first (fimpl/handle-opts (fsub/create) (ftup/create []) {})))))
