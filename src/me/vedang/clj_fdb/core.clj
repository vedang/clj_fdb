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

  and clears the key from the db. Returns nil."
  ([^TransactionContext tc k]
   (let [k-ba (build-byte-array k)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-key tr k-ba)))))
  ([^TransactionContext tc s k]
   (let [k-ba (build-byte-array s k)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-key tr k-ba))))))


(defn get-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to fetch or a Subspace `r`
  - IF `r` is a Subspace, can also accept `t`, a Tuple within that Subspace.
  - `keyfn` and `valfn`, to transform the key/value to the correct format.

  and returns a map of key/value pairs.

  Note that this function is greedy and forces the evaluation of the
  entire iterable. Use with care. If you want to get a lazy iterator,
  use the underlying get-range functions from `ftr` or `fsubspace`
  namespaces."
  ([^TransactionContext tc r keyfn valfn]
   (let [rg (condp instance? r
              Range r
              Subspace (fsubspace/range r)
              (throw (IllegalArgumentException.
                      "r should be either of type Range or of type Subspace")))]
     (ftr/run tc
       (fn [^Transaction tr]
         (reduce (fn [acc ^KeyValue kv]
                   (assoc acc (keyfn (.getKey kv)) (valfn (.getValue kv))))
                 {}
                 (ftr/get-range tr rg))))))
  ([^TransactionContext tc r t keyfn valfn]
   (if (and (instance? Subspace r) (instance? Tuple t))
     (get-range tc (fsubspace/range r t) keyfn valfn)
     (throw (IllegalArgumentException.
             "r should be of type Subspace and t should be of type Tuple")))))


(defn clear-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to be cleared `rg`

  and clears the range from the db. Returns nil."
  ([^TransactionContext tc r]
   (let [rg (condp instance? r
              Range r
              Subspace (fsubspace/range r)
              (throw (IllegalArgumentException.
                      "r should be either of type Range or of type Subspace")))]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-range tr rg)))))
  ([^TransactionContext tc r t]
   (if (and (instance? Subspace r) (instance? Tuple t))
     (clear-range tc (fsubspace/range r t))
     (throw (IllegalArgumentException.
             "r should be of type Subspace and t should be of type Tuple")))))
