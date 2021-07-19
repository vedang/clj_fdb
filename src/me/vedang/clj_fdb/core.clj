(ns me.vedang.clj-fdb.core
  (:refer-clojure :exclude [get set range])
  (:require [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb KeyValue Range Transaction TransactionContext]
           com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple
           java.lang.IllegalArgumentException))

(def default-opts
  "The default options to be passed into any options map"
  {})


(defn set
  "Takes the following:
  - TransactionContext `tc`
  - key to be stored `k` (should be byte-array, or convertible to byte-array)
  - value to be stored `v` (should be byte-array, or convertible to byte-array)
  - `Subspace` `s` under which the key will be stored
  - `opts` : unused at the moment, will support options like `:async?`
  in a later release.

  Returns nil."
  {:arglists '([tc k v] [tc s k v] [tc k v opts] [tc s k v opts])}
  ([^TransactionContext tc k v]
   (set tc nil k v default-opts))
  ([^TransactionContext tc arg1 arg2 arg3]
   (let [[s k v opts] (fimpl/handle-opts arg1 arg2 arg3)]
     (set tc s k v opts)))
  ([^TransactionContext tc s k v _opts]
   (let [k-ba (fimpl/encode s k)
         v-ba (fimpl/encode v)]
     (ftr/run tc (fn [^Transaction tr] (ftr/set tr k-ba v-ba))))))


(defn get
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k` (should be byte-array, or convertible to byte-array)
  - `Subspace` `s`, if you want to store the key under one.

  The opts map supports the following arguments:
  - Function `valfn` for converting the return value from byte-array
  to something else. Note that the byte-array is always sent through
  the `fimpl/decode` function first."
  {:arglists '([tc k] [tc s k] [tc k opts] [tc s k opts])}
  ([^TransactionContext tc k]
   (get tc k default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s k opts] (fimpl/handle-opts arg1 arg2)]
     (get tc s k opts)))
  ([^TransactionContext tc s k opts]
   (let [valfn (comp (:valfn opts identity) fimpl/decode)
         k-ba (fimpl/encode s k)
         v-ba (ftr/read tc (fn [^Transaction tr] (deref (ftr/get tr k-ba))))]
     (when v-ba (valfn v-ba)))))


(defn clear
  "Takes the following:
  - TransactionContext `tc`
  - key to be cleared `k`
  - `opts` : unused at the moment, will support options like `:async?`
  in a later release.

  and clears the key from the db. Returns nil."
  {:arglists '([tc k] [tc s k] [tc k opts] [tc s k opts])}
  ([^TransactionContext tc k]
   (clear tc nil k default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s k opts] (fimpl/handle-opts arg1 arg2)]
     (clear tc s k opts)))
  ([^TransactionContext tc s k _opts]
   (let [k-ba (fimpl/encode s k)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-key tr k-ba))))))

(defn range
  "Return a range according to the input arguments.

  At the moment, this should be considered as a helper function for
  `get-range` and `clear-range`. You should ideally never need to use
  it directly, even though it is in the core namespace."
  ([arg1]
   (cond
     (instance? Range arg1) arg1
     (vector? arg1) (ftup/range (ftup/create arg1))
     (instance? Tuple arg1) (ftup/range arg1)
     (instance? Subspace arg1) (fsub/range arg1)
     :else (throw (IllegalArgumentException.
                   "Cannot create a range from this input"))))
  ([arg1 arg2]
   (if arg1
     (let [s (cond
               (vector? arg1) (fsub/create arg1)
               (instance? Subspace arg1) arg1
               :else (throw (IllegalArgumentException.
                             "Arg1 should be of type Subspace")))
           t (cond
               (vector? arg2) (ftup/create arg2)
               (instance? Tuple arg2) arg2
               :else (throw (IllegalArgumentException.
                             "Arg2 should be of type Tuple")))]
       (fsub/range s t))
     (range arg2))))


(defn get-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to fetch or a Subspace `r-or-s`
  - IF `r-or-s` is a Subspace, can also accept `t`, a Tuple within that Subspace
  - `keyfn` and `valfn`, to transform the key/value to the correct format.

  and returns a map of key/value pairs.

  Note that this function is greedy and forces the evaluation of the
  entire iterable. Use with care. If you want to get a lazy iterator,
  use the underlying get-range functions from `ftr` or `fsub`
  namespaces."
  {:arglists '([tc rnge] [tc subspace] [tc k opts] [tc s k] [tc s k opts])}
  ([^TransactionContext tc arg1]
   (get-range tc arg1 default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s k opts] (fimpl/handle-opts arg1 arg2)]
     (get-range tc s k opts)))
  ([^TransactionContext tc s k opts]
   (let [rg (range s k)
         key-decoder (if (or s (instance? Subspace k))
                       (partial fimpl/decode (or s k))
                       fimpl/decode)
         keyfn (comp (:keyfn opts identity) key-decoder)
         valfn (comp (:valfn opts identity) fimpl/decode)]
     (ftr/read tc
               (fn [^Transaction tr]
                 (reduce (fn [acc ^KeyValue kv]
                           (assoc acc
                                  (keyfn (.getKey kv))
                                  (valfn (.getValue kv))))
                         {}
                         (ftr/get-range tr rg)))))))


(defn clear-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to be cleared `rg`
  - `opts` : unused at the moment, will support options like `:async?`
  in a later release.

  and clears the range from the db. Returns nil."
  {:arglists '([tc r] [tc s t] [tc r opts] [tc s t opts])}
  ([^TransactionContext tc r]
   (clear-range tc nil r default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s t opts] (fimpl/handle-opts arg1 arg2)]
     (clear-range tc s t opts)))
  ([^TransactionContext tc s t _opts]
   (let [rg (range s t)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-range tr rg))))))
