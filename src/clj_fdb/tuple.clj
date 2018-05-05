(ns clj-fdb.tuple
  (:refer-clojure :exclude [range])
  (:import com.apple.foundationdb.Range
           com.apple.foundationdb.tuple.Tuple))

(defn from
  "Creates a new Tuple from a variable number of elements.

  Note: an empty number of arguments is a valid input to this
  function, it creates an empty Tuple."
  [& args]
  (Tuple/from (into-array args)))

(defn from-bytes
  "Construct a new Tuple with elements decoded from a supplied byte array."
  [^"[B" ba]
  (Tuple/fromBytes ^"[B" ba))

(defn get-items
  "Gets the unserialized contents of this Tuple."
  [^Tuple t]
  (.getItems t))

(defn pack
  "Get an encoded representation of this Tuple."
  [^Tuple t]
  (.pack t))

(defn ^Range range
  "Returns a range representing all keys that encode Tuples strictly
  starting with this Tuple."
  [^Tuple t]
  (.range t))
