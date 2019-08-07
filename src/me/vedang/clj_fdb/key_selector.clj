(ns me.vedang.clj-fdb.key-selector
  (:refer-clojure :exclude [key])
  (:import com.apple.foundationdb.KeySelector))


(defn last-less-than
  "Creates a KeySelector that picks the last key less than the parameter"
  [^"[B" key]
  (KeySelector/lastLessThan key))


(defn last-less-or-equal
  "Returns a KeySelector that picks the last key less than or equal to the parameter."
  [^"[B" key]
  (KeySelector/lastLessOrEqual key))


(defn first-greater-than
  "Creates a KeySelector that picks the first key greater than the parameter"
  [^"[B" key]
  (KeySelector/firstGreaterThan key))


(defn first-greater-or-equal
  "Creates a KeySelector that picks the first key greater than or equal to the parameter"
  [^"[B" key]
  (KeySelector/firstGreaterOrEqual key))


(defn add
  "Returns a new KeySelector offset by a given number of keys from this one.
  https://apple.github.io/foundationdb/developer-guide.html#key-selectors."
  [^KeySelector key-selector offset]
  (.add key-selector offset))


(defn get-key
  "Returns a copy of the key that serves as the anchor for this KeySelector.
  This is not the key to which this KeySelector would resolve to."
  [^KeySelector key-selector]
  (.getKey key-selector))


(defn get-offset
  "Returns the key offset parameter for this KeySelector."
  [^KeySelector key-selector]
  (.getOffset key-selector))
