(ns me.vedang.clj-fdb.tuple.tuple-test
  (:require [clojure.test :refer [deftest is]]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import java.util.UUID))

(deftest tuple-size-tests
  (is (= 0 (.size (ftup/from))))
  (is (= 3 (.size (ftup/from 42 43 44))))
  (is (= 0 (.getPackedSize (ftup/from))))
  (is (= 38 (-> (ftup/from)
                (.add "4e1f71d3-728e-4d70-9996-b6aaea1fc02a")
                .getPackedSize)))
  (is (= 28 (-> (ftup/from)
                (.add "eqjry34s5ej8rbs1r6kyppmm0z")
                .getPackedSize)))
  (is (= 17 (-> (ftup/from)
                (.add (UUID/randomUUID))
                .getPackedSize))))


(deftest tuple-tests
  (is (= [] (ftup/get-items (ftup/from))))
  (is (= [42] (ftup/get-items (ftup/from 42))))
  (is (= [42 43 44] (ftup/get-items (.add (.add (.add (ftup/from) 42) 43) 44))))

  (is (= [] (ftup/get-items (ftup/from-bytes (ftup/pack (ftup/from))))))
  (is (= [42] (ftup/get-items (ftup/from-bytes (ftup/pack (ftup/from 42))))))

  (let [id #uuid "4e1f71d3-728e-4d70-9996-b6aaea1fc02a"]
    (is (= [id] (-> (ftup/from)
                    (.add id)
                    ftup/pack
                    ftup/from-bytes
                    ftup/get-items)))
    (is (= [id] (-> id
                    ftup/from
                    ftup/pack
                    ftup/from-bytes
                    ftup/get-items)))))
