(ns me.vedang.clj-fdb.core
  (:refer-clojure :exclude [get set])
  (:require [byte-streams :as bs]
            [me.vedang.clj-fdb.subspace.subspace :as fsubspace]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb KeyValue Range Transaction TransactionContext]
           com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple
           java.lang.IllegalArgumentException))


(defn set
  "Takes the following:
  - TransactionContext `tc`
  - key to be stored `k`
  - key to be stored `v`

  and stores `v` against `k` in FDB. Returns nil.

  Optionally, you can pass in `:keyfn` and `:valfn` to transform the
  key/value to a byte-array. An exception is thrown if these fns don't
  return a byte-array. If no conversion fn is provided, we attempt to
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


(defn clear-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to be cleared `rg`

  and clears the range from the db. Returns nil."
  [^TransactionContext tc ^Range rg]
  (let [tr-fn (fn [^Transaction tr] (ftr/clear-range tr rg))]
    (ftr/run tc tr-fn)))


(defn set-subspaced-key
  "Takes the following:
  - TransactionContext `tc`
  - Subspace `s` which will be used to namespace the key
  - Tuple `t` will be used along with `s` to construct key
  - Value to be stored `v`

  and stores `v` against key constructed using `s` and `t` in DB. Returns nil.

  Optionally, you can pass in `:valfn` as follows:

  - `:valfn` to transform the `v` to a byte-array. If `:valfn` is not provided,
  `bs/to-byte-array` is used to transform `v`."
  ([^TransactionContext tc ^Subspace s ^Tuple t v
    & {:keys [valfn]
       :or {valfn bs/to-byte-array}}]
   (set tc t v
        :keyfn #(fsubspace/pack s %)
        :valfn valfn)))


(defn get-subspaced-key
  "Takes the following:
  - TransactionContext `tc`
  - Subspace `s` which will be used to namespace the key
  - Tuple `t` will be used along with `s` to construct key

  and returns byte-array `v` against key constructed using `s` and `t`.

  Optionally, you can pass in `:valfn` as follows:

  - `:valfn` should accept a byte-array and convert it as desired by the caller.
  If no `:valfn` is provided, we return the byte-array as is."
  ([^TransactionContext tc ^Subspace s ^Tuple t
    & {:keys [valfn]
       :or {valfn identity}}]
   (get tc t
        :keyfn #(fsubspace/pack s %)
        :valfn valfn)))


(defn clear-subspaced-key
  "Takes the following:
  - TransactionContext `tc`
  - Subspace `s` which will used to namespace the key
  - Tuple `t` will be used along with `s` to construct key

  and clears the Subspaced key from db. Returns nil."
  [^TransactionContext tc ^Subspace s ^Tuple t]
  (clear tc t
         :keyfn #(fsubspace/pack s %)))


(defn get-subspaced-range
  "Takes the following:
  - TransactionContext `tc`
  - Subspace `s` which will used to namespace the key
  - Tuple `t` will be used along with `s` to construct key

  and returns a map of key/value pairs (byte-array->byte-array).

  Optionally, you can pass in `:keyfn` and `:valfn` as follows:

  - `:keyfn` can be passed to transform key to correct format.
  If no `:keyfn` is provided, we return the byte-array as is.
  - `:valfn` can be passed to tranform value to correct format.
  If no `:valfn` is provided, we return the byte-array as is."
  [^TransactionContext tc ^Subspace s ^Tuple t
   & {:keys [keyfn valfn]
      :or {keyfn identity
           valfn identity}}]
  (let [subspaced-range (fsubspace/range s t)]
    (get-range tc subspaced-range
               :keyfn keyfn
               :valfn valfn)))


(defn clear-subspaced-range
  "Takes the following:
  - TransactionContext `tc`
  - Subspace `s` which will used to namespace the key

  and clears the range from db. Returns nil.

  Optionally, you can pass in `t` as follows:

  - `t` will be used along with `s` to construct namespaced key. `t`
  should be of type `Tuple`."
  ([^TransactionContext tc ^Subspace s]
   (let [subspaced-range (fsubspace/range s)]
     (clear-range tc subspaced-range)))
  ([^TransactionContext tc ^Subspace s ^Tuple t]
   (let [subspaced-range (fsubspace/range s t)]
     (clear-range tc subspaced-range))))
