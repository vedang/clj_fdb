(ns me.vedang.clj-fdb.subspace.subspace
  (:refer-clojure :exclude [contains? range get])
  (:require [me.vedang.clj-fdb.tuple.tuple :as ftup])
  (:import com.apple.foundationdb.Range
           com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple))

(defn ^Subspace create
  "Constructor for a subspace formed with the specified prefix Tuple."
  ([prefix]
   (cond
     (vector? prefix) (Subspace. (apply ftup/from prefix))
     (instance? Tuple prefix) (Subspace. prefix)
     :else (throw (IllegalArgumentException.
                   "Don't know how to create Subspace from input"))))
  ([] (Subspace.)))


(defn contains?
  "Tests whether the specified key starts with this Subspace's prefix,
  indicating that the Subspace logically contains key."
  [^Subspace s ^"[B" key]
  (.contains s key))


(defn ^Range range
  "Gets a Range respresenting all keys strictly in the Subspace.

  If a tuple is passed, gets a Range representing all keys in the
  Subspace strictly starting with the specified Tuple."
  ([^Subspace s ^Tuple t]
   (.range s t))
  ([^Subspace s]
   (.range s)))


(defn ^"[B" pack
  "Gets the key encoding the prefix used for this Subspace.
  If a tuple is passed, key encoding is suffixed with passed tuple. If
  a non-tuple is passed, it is converted to a tuple and used as a
  suffix."
  ([^Subspace s]
   (.pack s))
  ([^Subspace s t]
   (condp instance? t
     Tuple (.pack s ^Tuple t)
     (.pack s ^Tuple (ftup/from t)))))


(defn ^Tuple unpack
  "Gets the Tuple encoded by the given key, with this Subspace's
  prefix Tuple and raw prefix removed."
  [^Subspace s ^"[B" key]
  (.unpack s key))


(defn ^"[B" get-key
  "Gets the key encoding the prefix used for this Subspace."
  [^Subspace s]
  (.getKey s))


(defn ^"[B" get
  "Gets a new subspace which is equivalent to this subspace with its
  prefix Tuple extended by the specified Tuple."
  [^Subspace s ^Tuple t]
  (.get s t))
