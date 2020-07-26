(ns me.vedang.clj-fdb.key-selector-test
  (:require
    [byte-streams :as bs]
    [clojure.test :refer :all]
    [me.vedang.clj-fdb.FDB :as cfdb]
    [me.vedang.clj-fdb.core :as fc]
    [me.vedang.clj-fdb.key-selector :refer :all]
    [me.vedang.clj-fdb.transaction :as ftr]))


(deftest test-constructors-and-getters
  (testing "Test the getter functions"
    (let [key-selectors-and-expected-results
          [{:ks (last-less-than (.getBytes "A"))
            :key "A" :offset 0}
           {:ks (last-less-or-equal (.getBytes "B"))
            :key "B" :offset 0}
           {:ks (first-greater-than (.getBytes "C"))
            :key "C" :offset 1}
           {:ks (first-greater-or-equal (.getBytes "D"))
            :key "D" :offset 1}]]
      (doseq [ks key-selectors-and-expected-results]
        (is (= (bs/to-string (get-key (:ks ks))) (:key ks))
            (= (get-offset (:ks ks)) (:offset ks)))))))


(deftest test-add
  (let [ks-1 (last-less-than (.getBytes "A"))
        new-ks-1 (add ks-1 10)
        ks-2 (first-greater-than (.getBytes "A"))
        new-ks-2 (add ks-2 -5)]
    (is (= (get-offset new-ks-1) 10)
        (= (get-offset new-ks-2) -4))))
