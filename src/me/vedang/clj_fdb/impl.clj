(ns me.vedang.clj-fdb.impl
  "Helper functions for implementing core functionality. This namespace
  should not be treated as public. Functions inside this namespace are
  subject to change and meant as helper functions for the core."
  (:require [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple))

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
