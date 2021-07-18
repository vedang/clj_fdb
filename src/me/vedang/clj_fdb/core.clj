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
     (nil? k) (ftup/pack (ftup/from)) ;; Handle nil as empty Tuple
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
       (nil? s) (encode k')
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
     (nil? s) (decode code)
     (instance? Subspace s) (ftup/get-items (fsub/unpack s code))
     (vector? s) (ftup/get-items (fsub/unpack (fsub/create s) code))
     ;; I don't know how to convert this back, will let caller deal
     ;; with it.
     :else code)))


(def default-opts
  "The default options to be passed into any options map"
  {})


(defn handle-opts
  "This function makes it easy to deal with the following multiple arity
  pattern:

  {:arglists '([tc k v] [tc s k v] [tc k v opts] [tc s k v opts])}

  In this case, we have clashing arities due to default opts vs
  explicitly passed opts. The basic check is that if the argument
  passed in is a map, it is considered to represent opts. In this
  case, the missing argument is sent back as nil."
  [& args]
  (if (map? (last args))
    (cons nil args) ; opts map is passed, first argument is nil
    (concat args '({})))) ; opts map is not passed, use default opts


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
   (let [[s k v opts] (handle-opts arg1 arg2 arg3)]
     (set tc s k v opts)))
  ([^TransactionContext tc s k v _opts]
   (let [k-ba (encode s k)
         v-ba (encode v)]
     (ftr/run tc (fn [^Transaction tr] (ftr/set tr k-ba v-ba))))))


(defn get
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k` (should be byte-array, or convertible to byte-array)
  - `Subspace` `s`, if you want to store the key under one.

  The opts map supports the following arguments:
  - Function `valfn` for converting the return value from byte-array
  to something else."
  {:arglists '([tc k] [tc s k] [tc k opts] [tc s k opts])}
  ([^TransactionContext tc k]
   (get tc k default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s k opts] (handle-opts arg1 arg2)]
     (get tc s k opts)))
  ([^TransactionContext tc s k opts]
   (let [valfn (:valfn opts decode)
         k-ba (encode s k)
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
   (let [[s k opts] (handle-opts arg1 arg2)]
     (clear tc s k opts)))
  ([^TransactionContext tc s k _opts]
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
   (let [[s k opts] (handle-opts arg1 arg2)]
     (get-range tc s k opts)))
  ([^TransactionContext tc s k opts]
   (let [rg (range s k)
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
  - `opts` : unused at the moment, will support options like `:async?`
  in a later release.

  and clears the range from the db. Returns nil."
  {:arglists '([tc r] [tc s t] [tc r opts] [tc s t opts])}
  ([^TransactionContext tc r]
   (clear-range tc nil r default-opts))
  ([^TransactionContext tc arg1 arg2]
   (let [[s t opts] (handle-opts arg1 arg2)]
     (clear-range tc s t opts)))
  ([^TransactionContext tc s t _opts]
   (let [rg (range s t)]
     (ftr/run tc (fn [^Transaction tr] (ftr/clear-range tr rg))))))
