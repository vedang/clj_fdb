(ns clj-fdb.subspace-test
  (:require [clojure.test :refer :all]
            [byte-streams :as bs]
            [clj-fdb.FDB :as cfdb]
            [clj-fdb.core :as fc]
            [clj-fdb.subspace.subspace :as fss]
            [clj-fdb.tuple.tuple :as ftup]
            [clj-fdb.internal.util :as u]))


(deftest test-prefixed-subspace
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        random-prefix (str "prefixed-subspace-test:"
                           (u/rand-str 5))
        prefix-subspace (fss/create-subspace (ftup/from random-prefix))]
    (with-open [db (cfdb/open fdb)]
      (fc/set db
              (fss/pack prefix-subspace
                        (ftup/from "test-key"))
              "subspace value")
      (is (= (fc/get db
                     (fss/pack prefix-subspace
                               (ftup/from "test-key"))
                     :valfn #(bs/convert % String))
             "subspace value"))
      (is (fss/contains? prefix-subspace
                         (fss/pack prefix-subspace
                                   (ftup/from "test-key"))))
      (is (= (fss/unpack prefix-subspace
                         (fss/pack prefix-subspace
                                   (ftup/from)))
             (ftup/from)))
      (is (= (fss/unpack prefix-subspace
                         (fss/pack prefix-subspace
                                   (ftup/from "test")))
             (ftup/from "test"))))))
