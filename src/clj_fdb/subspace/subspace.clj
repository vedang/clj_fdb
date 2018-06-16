(ns clj-fdb.subspace.subspace
  (:import com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.Range
           com.apple.foundationdb.tuple.Tuple))


(defn ^Subspace create-subspace
  "Constructor for a subspace formed with the specified prefix Tuple."
  ([^Tuple prefix]
   (Subspace. prefix))
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
  [^Subspace s]
  (.pack s))


(defn ^Tuple unpack
  [^Subspace s ^"[B" key]
  (.unpack s key))
