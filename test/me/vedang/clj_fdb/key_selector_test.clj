(ns me.vedang.clj-fdb.key-selector-test
  (:require
    [byte-streams :as bs]
    [clojure.test :refer [deftest is testing]]
    [me.vedang.clj-fdb.key-selector :as sut]))


(deftest constructors-and-getters-tests
  (testing "Test getter functions for key-selectors"
    (let [key-selectors-and-expected-results
          [{:ks (sut/last-less-than (.getBytes "A"))
            :key "A"
            :offset 0}
           {:ks (sut/last-less-or-equal (.getBytes "B"))
            :key "B"
            :offset 0}
           {:ks (sut/first-greater-than (.getBytes "C"))
            :key "C"
            :offset 1}
           {:ks (sut/first-greater-or-equal (.getBytes "D"))
            :key "D"
            :offset 1}]]
      (doseq [ks key-selectors-and-expected-results]
        (is (=  (:key ks) (bs/to-string (sut/get-key (:ks ks))))
            (=  (:offset ks) (sut/get-offset (:ks ks))))))))


(deftest add-offset-to-key-selectors-tests
  (let [ks-1 (sut/last-less-than (.getBytes "A"))
        new-ks-1 (sut/add ks-1 10)
        ks-2 (sut/first-greater-than (.getBytes "A"))
        new-ks-2 (sut/add ks-2 -5)]
    (is (=  10 (sut/get-offset new-ks-1))
        (=  -4 (sut/get-offset new-ks-2)))))
