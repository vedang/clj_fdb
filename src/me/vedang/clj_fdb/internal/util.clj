(ns me.vedang.clj-fdb.internal.util
  (:require [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.directory.directory :as fdir]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import com.apple.foundationdb.Database))

(let [alphabet (vec "abcdefghijklmnopqrstuvwxyz0123456789")]
  (defn rand-str
    "Generate a random string of length l"
    [l]
    (loop [n l res (transient [])]
      (if (zero? n)
        (apply str (persistent! res))
        (recur (dec n) (conj! res (alphabet (rand-int 36))))))))


(def ^:dynamic *test-prefix* nil)


(defn- clear-all-with-prefix
  "Helper fn to ensure sanity of DB"
  [prefix]
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        rg (ftup/range (ftup/from prefix))]
    (with-open [^Database db (cfdb/open fdb)]
      (fdir/remove! db [prefix])
      (fc/clear-range db rg))))


(defn test-fixture
  [test]
  (let [random-prefix (str "testcycle:" (rand-str 5))]
    (binding [*test-prefix* random-prefix]
      (test))
    (clear-all-with-prefix random-prefix)))
