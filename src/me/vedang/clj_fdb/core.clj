(ns me.vedang.clj-fdb.core
  (:refer-clojure :exclude [get set])
  (:require [byte-streams :as bs]
            [me.vedang.clj-fdb.internal.byte-conversions
             :refer [build-byte-array byte-array-class]]
            [me.vedang.clj-fdb.subspace.subspace :as fsubspace]
            [me.vedang.clj-fdb.transaction :as ftr])
  (:import [com.apple.foundationdb KeyValue Range Transaction TransactionContext]
           com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple
           java.lang.IllegalArgumentException))


(defn set
  "Takes the following:
  - TransactionContext `tc`
  - key to be stored `k` (should be byte-array, or convertible to byte-array)
  - value to be stored `v` (should be byte-array, or convertible to byte-array)

  and stores `v` against `k` in FDB. Returns nil.

  Optionally, you can also pass a `Subspace` `s` under which the key
  will be stored. "
  ([^TransactionContext tc k v]
   (let [k-ba (build-byte-array k)
         v-ba (build-byte-array v)]
     (ftr/run tc (fn [^Transaction tr] (ftr/set tr k-ba v-ba)))))
  ([^TransactionContext tc s k v]
   (let [k-ba (build-byte-array s k)
         v-ba (build-byte-array v)]
     (ftr/run tc (fn [^Transaction tr] (ftr/set tr k-ba v-ba))))))


(defn get
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k` (should be byte-array, or convertible to byte-array)
  - Function `parsefn` taking the stored byte-array at `k` and
  converting it to the appropriate return value `v`

  Optionally, you can also pass a `Subspace` `s`, under which the key
  is stored."
  ([^TransactionContext tc k parsefn]
   (let [k-ba (build-byte-array k)
         v-ba (ftr/run tc (fn [^Transaction tr] (deref (ftr/get tr k-ba))))]
     (when v-ba (parsefn v-ba))))
  ([^TransactionContext tc s k parsefn]
   (get tc (build-byte-array s k) parsefn)))


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
   (get tc (fsubspace/pack s t) valfn)))


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
