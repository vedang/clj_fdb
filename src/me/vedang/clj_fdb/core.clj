(ns me.vedang.clj-fdb.core
  (:refer-clojure :exclude [get set range])
  (:require [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.key-selector :as sut]
            [me.vedang.clj-fdb.mutation-type :as fmut]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb KeySelector KeyValue Range Transaction TransactionContext]
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

  The `opts` map supports the following arguments:

  - Function `valfn` for converting the return value from byte-array
  to something else. Note that the byte-array is always sent through
  the `fimpl/decode` function first. (So if you have stored a Tuple in
  FDB, the valfn will be passed a vector of elements instead of a FDB
  Tuple Object)

  Returns the value stored at `k`, nil if no value exists."
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

  Clears the key from the db. Returns nil."
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

(defn- create-range-args [arg1 arg2 {:keys [skip]}]
  (if (instance? KeySelector arg1)
    [(cond-> arg1 skip (sut/add skip)) arg2]
    [(range arg1 arg2)]))

(defn- get-key-decoder [arg1 arg2]
  (cond (instance? KeySelector arg1)
        fimpl/decode
        (or arg1 (instance? Subspace arg2))
        (partial fimpl/decode (or arg1 arg2))
        :else fimpl/decode))

(defn get-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to fetch `rng`, Subspace `subspace` or a Keyselector
  pair of `begin` and `end`.
  - In the case of a `subspace`, can also accept `t`, a Tuple within
  that Subspace

  The `opts` map takes the following option at the moment:
  - `keyfn` :
  - `valfn` : Functions to transform the key/value to the correct format.
  - `limit` : limit the number of returned tuples
  - `skip`  : skip a number of keys from the beginning of the range
              (currently only works with KeySelectors)

  Note that the byte-arrays are always sent through the `fimpl/decode`
  function first. (So if you have stored a Tuple in FDB, the `valfn`
  will be passed a vector of elements instead of a FDB Tuple Object)

  Note that this function is greedy and forces the evaluation of the
  entire iterable. Use with care. If you want to get a lazy iterator,
  use the underlying get-range functions from `ftr` or `fsub`
  namespaces.

  Returns a map of key/value pairs."
  {:arglists '([tc rnge] [tc subspace] [tc k opts] [tc s k]
               [tc begin end] [tc s k opts] [tc begin end opts])}
  ([^TransactionContext tc arg1]
   (get-range tc arg1 default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[arg1 arg2 opts] (fimpl/handle-opts arg1 arg2)]
     (get-range tc arg1 arg2 opts)))
  ([^TransactionContext tc arg1 arg2 {:keys [limit keyfn valfn] :as opts}]
   (let [range-args (cond-> (create-range-args arg1 arg2 opts)
                      limit (conj limit))
         key-decoder (get-key-decoder arg1 arg2)
         keyfn (cond->> key-decoder
                 keyfn (comp keyfn))
         valfn (cond->> fimpl/decode
                 valfn (comp valfn))]
     (ftr/read tc
               (fn [^Transaction tr]
                 (reduce (fn [acc ^KeyValue kv]
                           (assoc acc
                                  (keyfn (.getKey kv))
                                  (valfn (.getValue kv))))
                         {}
                         (apply ftr/get-range tr range-args)))))))

(defn clear-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to be cleared `rg`
  - `opts` : unused at the moment, will support options like `:async?`
  in a later release.

  Clears the range from the db. Returns nil."
  {:arglists '([tc r] [tc s t] [tc r opts] [tc s t opts])}
  ([^TransactionContext tc r]
   (clear-range tc nil r default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s t opts] (fimpl/handle-opts arg1 arg2)]
     (clear-range tc s t opts)))
  ([^TransactionContext tc s t _opts]
   (let [rg (range s t)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-range tr rg))))))


(let [empty-byte-array (byte-array 0)]
  (defn mutate!
    "An atomic operation is a single database command that carries out
  several logical steps: reading the value of a key, performing a
  transformation on that value, and writing the result."
    {:arglists '([tc mut k] [tc mut s k] [tc mut k byte-op] [tc mut s k byte-op])}
    ([^TransactionContext tc mut arg1]
     (if (instance? Subspace arg1)
       (mutate! tc mut arg1 (ftup/from) empty-byte-array)
       (mutate! tc mut nil arg1 empty-byte-array)))
    ([^TransactionContext tc mut arg1 arg2]
     (if (instance? Subspace arg1)
       ;; treat arg2 as the key, and send an empty-byte-array.
       (mutate! tc mut arg1 arg2 empty-byte-array)
       ;; treat arg2 as the param
       (mutate! tc mut nil arg1 arg2)))
    ([^TransactionContext tc mut s k byte-op]
     (ftr/run tc
       (fn [^Transaction tr]
         (ftr/mutate! tr
                      (fmut/mutation mut)
                      (fimpl/encode s k)
                      (fimpl/encode byte-op)))))))
