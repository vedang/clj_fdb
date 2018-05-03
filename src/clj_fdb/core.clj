(ns clj-fdb.core
  (:require [byte-streams :as bs]
            [clj-fdb.byte-conversions :refer [byte-array-class]]
            [clj-fdb.transaction :as ftr])
  (:import [com.apple.foundationdb KeyValue Range Transaction TransactionContext]
           java.lang.IllegalArgumentException))

(defn set
  "Takes the following:
  - TransactionContext `tc`
  - key to be stored `k`
  - key to be stored `v`

  and stores `v` against `k` in FDB. Returns nil.

  Optionally, you can pass in `:keyfn` and `:valfn` to transform the
  key/value to a byte-array. An exception is thrown if these fns don't
  return a byte-array. If no conversion fn is provided, we attemp to
  convert k/v to byte-array using `byte-streams/to-byte-array`."
  [^TransactionContext tc k v &
   {:keys [keyfn valfn]
    :or {keyfn bs/to-byte-array
         valfn bs/to-byte-array}}]
  (let [tr-fn (fn [^Transaction tr]
                (let [k-ba (keyfn k)
                      v-ba (valfn v)]
                  (when-not (instance? byte-array-class k-ba)
                    (throw (IllegalArgumentException.
                            "The provided Key Fn did not return a byte-array on input")))
                  (when-not (instance? byte-array-class v-ba)
                    (throw (IllegalArgumentException.
                            "The provided Val Fn did not return a byte-array on input")))
                  (ftr/set tr k-ba v-ba)))]
    (ftr/run tc tr-fn)))

(defn get
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k`

  and returns byte-array `v` against `k` in FDB.

  Optionally, you can pass in `:keyfn` and `:valfn` as follows:

  - `:keyfn` should take the key as input and transform it to a
  byte-array. An exception is thrown if the return value is not a
  byte-array. If no `:keyfn` is provided, we convert the key to a
  byte-array using `byte-streams/to-byte-array`.

  - `:valfn` should accept a byte-array and convert it as desired by
  the caller. If no `:valfn` is provided, we return the byte-array as
  is."
  [^TransactionContext tc k &
   {:keys [keyfn valfn]
    :or {keyfn bs/to-byte-array
         valfn identity}}]
  (let [tr-fn (fn [^Transaction tr]
                (let [k-ba (keyfn k)]
                  (when-not (instance? byte-array-class k-ba)
                    (throw (IllegalArgumentException.
                            "The provided Key Fn did not return a byte-array on input")))
                  (when-let [v (deref (ftr/get tr k-ba))]
                    (valfn v))))]
    (ftr/run tc tr-fn)))

(defn clear
  "Takes the following:
  - TransactionContext `tc`
  - key to be cleared `k`

  and clears the key from the db. Returns nil.

  Optionally, you can pass in `:keyfn` as follows:

  - `:keyfn` should take the key as input and transform it to a
  byte-array. An exception is thrown if the return value is not a
  byte-array. If no `:keyfn` is provided, we convert the key to a
  byte-array using `byte-streams/to-byte-array`."
  [^TransactionContext tc k &
   {:keys [keyfn]
    :or {keyfn bs/to-byte-array}}]
  (let [tr-fn (fn [^Transaction tr]
                (let [k-ba (keyfn k)]
                  (when-not (instance? byte-array-class k-ba)
                    (throw (IllegalArgumentException.
                            "The provided Key Fn did not return a byte-array on input")))
                  (ftr/clear-key tr k-ba)))]
    (ftr/run tc tr-fn)))

(defn get-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to fetch `rg`

  and returns a map of key/value pairs (byte-array->byte-array).

  Optionally, you can pass in `:keyfn` and `:valfn` to transform the
  key/value to the correct format. `:keyfn` should accept a byte-array
  representing the key, `:valfn` should accept a byte-array
  representing the value."
  [^TransactionContext tc ^Range rg &
   {:keys [keyfn valfn]
    :or {keyfn identity
         valfn identity}}]
  (let [tr-fn (fn [^Transaction tr]
                (reduce (fn [acc ^KeyValue kv]
                          (assoc acc
                                 (keyfn (.getKey kv)) (valfn (.getValue kv))))
                        {}
                        (ftr/get-range tr rg)))]
    (ftr/run tc tr-fn)))
