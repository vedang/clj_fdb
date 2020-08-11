(ns me.vedang.clj-fdb.tuple-io-test
  (:require
   [me.vedang.clj-fdb.tuple.converters :as converter]   
   [clojure.test :refer :all]
   [me.vedang.clj-fdb.FDB :as cfdb]
   [me.vedang.clj-fdb.tuple-io :as fio]

   [me.vedang.clj-fdb.internal.util :as u]
   [me.vedang.clj-fdb.range :as frange]
   [me.vedang.clj-fdb.subspace.subspace :as fsubspace]
   [me.vedang.clj-fdb.transaction :as ftr]
   [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb   Database  Transaction Range]))


(use-fixtures :each u/test-fixture)


(deftest test-set-get-int
  (testing "Test the best-case path for `fio/set` and `fio/get`"
    (let [k-t (ftup/from u/*test-prefix* "foox")
          value-to-store (int 32)
          
          v-t (converter/encode-int value-to-store)
          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          _ (with-open [^Database db (cfdb/open fdb)]
              (fio/set db k-t v-t))
          
          bval (with-open [^Database db (cfdb/open fdb)](fio/get db k-t))
          
          v2 (converter/decode-int bval)
                    ]
        (is (= v2 value-to-store))
      ) ) )
        

(deftest test-set-get-string
  (testing "Test the best-case path for `fio/set` and `fio/get`"
    (let [k-t (ftup/from u/*test-prefix* "foox2")
          value-to-store "Helloo I am a string"

          v-t (.getBytes value-to-store)
          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          _ (with-open [^Database db (cfdb/open fdb)]
              (fio/set db k-t v-t))
          
          bval (with-open [^Database db (cfdb/open fdb)] (fio/get db k-t))
          
          v2 (String. ^bytes bval)
        ]
      (is (= v2 value-to-store)))))



(deftest test-get-non-existent-key
  (testing "Test that `fio/get` on a non-existent key returns `nil`"
    (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
          k (ftup/from u/*test-prefix* "non-existent")]
      (with-open [^Database db (cfdb/open fdb)]
        (is (nil? (fio/get db k)))))))

(def data-pairs  [[["fruit" "date"] "brown"]
                  [["fruit" "apple mackintosh"]     "red"]  ; -note the key is "apple mackintosh" not ["apple" "mackintosh"]
                  [["fruit" "apple" "royal-gala"]   "red"]
                  [["fruit" "banana" "plantain"]    "green"]
                  [["fruit" "apple" "granny-smith"] "green"]
                  [["fruit"  "banana" "cavendish"] "yellow"]
                  [["vegetable" "squash" "butternut"] "yellow"]
                  [["vegetable" "squash" "pumpkin"] "yellow"]])

;[(ftup/from base-k-t k)  (.getBytes v)]
(deftest test-range-with-start-end
  (testing "Test the range query with start and end prefixes"
    (let [p-k-t (partial apply ftup/from  u/*test-prefix* )

          kv-pairs (map #(let [[k v] %] [(apply ftup/from  u/*test-prefix* k) (.getBytes ^String  v)]) data-pairs)

          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)


          _ (doseq [[k v]  kv-pairs] (with-open [^Database db (cfdb/open fdb)]
                               
                                       (fio/set db k v)))

          vv (with-open [^Database db (cfdb/open fdb)]
               (fio/get db (p-k-t ["fruit"  "banana" "cavendish"])))
          ; this  should be yellow
          color-of-cavnedish (String. ^bytes vv)
           begin      (ftup/pack (ftup/from u/*test-prefix* "f"))
          end        (ftup/pack (ftup/from u/*test-prefix* "v"))
          

          all-fruits (with-open [^Database db (cfdb/open fdb)] (fio/get-range db (frange/range begin end)))

        ]
      (is (= 6 (count all-fruits)))
      (is (= color-of-cavnedish "yellow")) )))



;--------------------------------
(deftest test-range-starts-with
  (testing "Test the range with starts-with "
    (let [p-k-t (partial apply ftup/from  u/*test-prefix*)


          kv-pairs (map #(let [[k v] %] [(apply ftup/from  u/*test-prefix* k) (.getBytes ^String v)]) data-pairs)

          fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)


          _ (doseq [[k v]  kv-pairs] (with-open [^Database db (cfdb/open fdb)]                                     
                                       (fio/set db k v)))
          val-bytes (with-open [^Database db (cfdb/open fdb)] (fio/get db (p-k-t ["fruit"  "banana" "cavendish"])))
          color-of-cavnedish (String. ^bytes val-bytes) 
          rng          (frange/starts-with ^bytes (ftup/pack (apply ftup/from u/*test-prefix* ["fruit" "apple"])))
          range-apples (with-open [^Database db (cfdb/open fdb)] (fio/get-range db rng))

]
      (is (= 2 (count range-apples)))
      (is (= color-of-cavnedish "yellow")))))






