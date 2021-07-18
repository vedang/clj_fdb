(ns me.vedang.clj-fdb.core
  (:refer-clojure :exclude [get set range])
  (:require [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import [com.apple.foundationdb KeyValue Range Transaction TransactionContext]
           com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple
           java.lang.IllegalArgumentException))


(def byte-array-class (class (byte-array 0)))


(defn ^"[B" encode
  "Takes input data and returns the byte-array representation of that data.
  Supports strings, vectors, Tuples, Subspaces, Directories. For any
  other type of key, please serialize to byte-array yourself."
  ([k]
   (cond
     (instance? byte-array-class k) k
     (instance? String k) (.getBytes ^String k "UTF-8")
     (instance? Tuple k) (ftup/pack ^Tuple k)
     (vector? k) (ftup/pack (ftup/create k))
     :else (throw (IllegalArgumentException.
                   "I don't know how to convert input data to a byte-array"))))
  ([s k]
   ;; DirectorySubspace also implements Subspace, so s can be either
   ;; of the two.
   (let [k' (if (vector? k) (ftup/create k) k)]
     (cond
       (instance? Subspace s) (fsub/pack s k')
       (vector? s) (fsub/pack (fsub/create s) k')
       :else (throw (IllegalArgumentException.
                     "I don't know how to convert input data to a byte-array"))))))


(defn decode
  "Takes a packed Tuple and returns the contents of the Tuple."
  ([^"[B" code]
   (try (when code (ftup/get-items (ftup/from-bytes code)))
        ;; I don't know how to convert this back, will let caller deal
        ;; with it.
        (catch IllegalArgumentException _ code)))
  ([s code]
   (cond
     (instance? Subspace s) (ftup/get-items (fsub/unpack s code))
     (vector? s) (ftup/get-items (fsub/unpack (fsub/create s) code))
     ;; I don't know how to convert this back, will let caller deal
     ;; with it.
     :else code)))

(defn set
  "Takes the following:
  - TransactionContext `tc`
  - key to be stored `k` (should be byte-array, or convertible to byte-array)
  - value to be stored `v` (should be byte-array, or convertible to byte-array)
  - `Subspace` `s` under which the key will be stored
  - `opts` : unused at the moment, will support options like `:async?` in a later release.

  Returns nil."
  ([^TransactionContext tc k v]
   (let [k-ba (encode k)
         v-ba (encode v)]
     (ftr/run tc (fn [^Transaction tr] (ftr/set tr k-ba v-ba)))))
  ([^TransactionContext tc s k v]
   (let [k-ba (encode s k)
         v-ba (encode v)]
     (ftr/run tc (fn [^Transaction tr] (ftr/set tr k-ba v-ba))))))


(defn get
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k` (should be byte-array, or convertible to byte-array)
  - `Subspace` `s`, if you want to store the key under one.

  The opts map supports the following arguments:
  - Function `parsefn` for converting the return value from byte-array
  to something else."
  {:arglists '([tc k] [tc s k] [tc k opts] [tc s k opts])}
  ([^TransactionContext tc k]
   (get tc k {}))
  ([^TransactionContext tc arg1 arg2]
   (let [[s k opts] (if (map? arg2) [nil arg1 arg2] [arg1 arg2 {}])
         parsefn (:parsefn opts decode)
         k-ba (if s (encode s k) (encode k))
         v-ba (ftr/read tc (fn [^Transaction tr] (deref (ftr/get tr k-ba))))]
     (when v-ba (parsefn v-ba))))
  ([^TransactionContext tc s k opts]
   (get tc (encode s k) opts)))


(defn clear
  "Takes the following:
  - TransactionContext `tc`
  - key to be cleared `k`

  and clears the key from the db. Returns nil."
  ([^TransactionContext tc k]
   (let [k-ba (encode k)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-key tr k-ba)))))
  ([^TransactionContext tc s k]
   (let [k-ba (encode s k)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-key tr k-ba))))))

(defn range
  "Return a range according to the input arguments.

  At the moment, this should be considered as a helper function for
  `get-range` and `clear-range`. You should ideally never need to use
  it directly, even though it is in the core namespace."
  ([arg1]
   (cond
     (instance? Range arg1) arg1
     (instance? Subspace arg1) (fsub/range arg1)
     (vector? arg1) (ftup/range (ftup/create arg1))
     :else (throw (IllegalArgumentException.
                   "Arg should be either a vector or of type Range or of type Subspace"))))
  ([arg1 arg2]
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
     (fsub/range s t))))


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
   (get-range tc arg1 {}))
  ([^TransactionContext tc arg1 arg2]
   (let [[s k opts] (if (map? arg2) [nil arg1 arg2] [arg1 arg2 {}])]
     (get-range tc s k opts)))
  ([^TransactionContext tc s k opts]
   (let [rg (if s (range s k) (range k))
         keyfn (:keyfn opts
                       (if (or s (instance? Subspace k))
                         (partial decode (or s k))
                         decode))
         valfn (:valfn opts decode)]
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

  and clears the range from the db. Returns nil."
  ([^TransactionContext tc r]
   (let [rg (condp instance? r
              Range r
              Subspace (fsub/range r)
              (throw (IllegalArgumentException.
                      "r should be either of type Range or of type Subspace")))]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-range tr rg)))))
  ([^TransactionContext tc s t]
   (if (and (instance? Subspace s) (instance? Tuple t))
     (clear-range tc (fsub/range s t))
     (throw (IllegalArgumentException.
             "s should be of type Subspace and t should be of type Tuple")))))
