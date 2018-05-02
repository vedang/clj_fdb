(ns clj-fdb.core
  (:require [byte-streams :as bs]
            [clj-fdb.transaction :as ftr])
  (:import [com.apple.foundationdb KeyValue Range Transaction TransactionContext]
           com.apple.foundationdb.tuple.Tuple))

;;; Load functions which teach byte-streams how to convert things to
;;; byte-arrays.
(load "byte_conversions")

(defn set
  "Takes the following:
  - TransactionContext `db`
  - key to be stored `k`
  - key to be stored `v`

  and stores `v` against `k` in FDB. Returns nil.

  Converts k/v to byte-array using byte-streams library."
  [^TransactionContext db k v]
  (let [tr-fn (fn [^Transaction tr]
                (ftr/set tr
                         (bs/to-byte-array k)
                         (bs/to-byte-array v)))]
    (ftr/run db tr-fn)))

(defn get
  "Takes the following:
  - TransactionContext `db`
  - key to be fetched `k`

  and returns byte-array `v` against `k` in FDB. Caller is responsible
  for de-structuring v correctly.

  Converts k to byte-array using the byte-streams library."
  [^TransactionContext db k]
  (let [tr-fn (fn [^Transaction tr]
                (deref (ftr/get tr (bs/to-byte-array k))))]
    (ftr/run db tr-fn)))

(defn clear
  "Takes the following:
  - TransactionContext `db`
  - key to be cleared `k`

  and clears the key from the db. Returns nil."
  [^TransactionContext db k]
  (let [tr-fn (fn [^Transaction tr]
                (ftr/clear-key tr (bs/to-byte-array k)))]
    (ftr/run db tr-fn)))

(defn get-range
  "Takes the following:
  - TransactionContext `db`
  - Range of keys to fetch `rg`

  and returns a map of key/value pairs (byte-array->byte-array).

  Optionally, you can pass in `:keyfn` and `:valfn` to transform the
  key/value to the correct format. `:keyfn` should accept a byte-array
  representing the key, `:valfn` should accept a byte-array
  representing the value."
  [^TransactionContext db ^Range rg &
   {:keys [keyfn valfn]
    :or {keyfn identity
         valfn identity}}]
  (let [tr-fn (fn [^Transaction tr]
                (reduce (fn [acc ^KeyValue kv]
                          (assoc acc
                                 (keyfn (.getKey kv)) (valfn (.getValue kv))))
                        {}
                        (ftr/get-range tr rg)))]
    (ftr/run db tr-fn)))
