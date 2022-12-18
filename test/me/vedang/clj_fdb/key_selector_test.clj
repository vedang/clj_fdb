(ns me.vedang.clj-fdb.key-selector-test
  (:require
   [byte-streams :as bs]
   [clojure.test :refer [deftest is testing]]
   [me.vedang.clj-fdb.key-selector :as fks]))


(deftest constructors-and-getters-tests
  (testing "Test getter functions for key-selectors"
    (let [key-selectors-and-expected-results
          [{:ks (fks/last-less-than (.getBytes "A"))
            :key "A"
            :offset 0}
           {:ks (fks/last-less-or-equal (.getBytes "B"))
            :key "B"
            :offset 0}
           {:ks (fks/first-greater-than (.getBytes "C"))
            :key "C"
            :offset 1}
           {:ks (fks/first-greater-or-equal (.getBytes "D"))
            :key "D"
            :offset 1}]]
      (doseq [ks key-selectors-and-expected-results]
        (is (=  (:key ks) (bs/to-string (fks/get-key (:ks ks))))
            (=  (:offset ks) (fks/get-offset (:ks ks))))))))


(deftest add-offset-to-key-selectors-tests
  (let [ks-1 (fks/last-less-than (.getBytes "A"))
        new-ks-1 (fks/add ks-1 10)
        ks-2 (fks/first-greater-than (.getBytes "A"))
        new-ks-2 (fks/add ks-2 -5)]
    (is (=  10 (fks/get-offset new-ks-1))
        (=  -4 (fks/get-offset new-ks-2)))))
