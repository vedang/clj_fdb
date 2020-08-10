(ns me.vedang.clj-fdb.converter-test
  (:require
[clojure.test :refer :all]
   [me.vedang.clj-fdb.tuple.converters :as converter]
   )
  )

(deftest long-converter-test
  (testing "long converter"
    (let [value-to-store  999
          bl (converter/encode-long value-to-store)
          
          l2 (converter/decode-long bl)
          
      ]
      (is (= l2 value-to-store))
      )))

(deftest int-converter-test
  (testing "int conveter test."
    (let [value-to-store  (int 999)
          bl (converter/encode-int value-to-store)
          l2 (converter/decode-int bl)
         ]
      (is (= l2 value-to-store)))))

; todo -- more converters