(ns clj-fdb.core
  (:require [byte-streams :as bs]
            [clj-fdb.transaction :as ftr])
  (:import [com.apple.foundationdb Transaction TransactionContext]))

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
