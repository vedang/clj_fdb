(ns me.vedang.clj-fdb.internal.byte-conversions
  (:require [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple))


(def byte-array-class (class (byte-array 0)))


(defn ^"[B" build-byte-array
  "Takes a key/value and returns the byte-array representation of that key.
  Out of the box, this library only supports strings, Tuples,
  Subspaces, Directories. For any other type of key, please serialize
  to byte-array yourself."
  ([k]
   (condp instance? k
     byte-array-class k
     String (.getBytes ^String k "UTF-8")
     Tuple (ftup/pack ^Tuple k)
     (throw (IllegalArgumentException.
             "I don't know how to convert input data to a byte-array"))))
  ([s k]
   (condp instance? s
     Subspace (fsub/pack s k)
     ;; I will add Directory layer support here soon.
     (throw (IllegalArgumentException.
             "I don't know how to convert input data to a byte-array")))))
