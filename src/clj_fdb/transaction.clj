(ns clj-fdb.transaction
  (:import clojure.lang.IFn
           [com.apple.foundationdb Range Transaction TransactionContext]
           java.util.function.Function))

(defn- as-java-fn
  "Takes a clojure fn and returns a `reify`'d version which implements
  `java.util.function.Function`.

  Note: The fn should accept a single argument."
  [f]
  (reify Function
    (apply [this arg]
      (f arg))))

(defn run
  "Takes a `TransactionContext` and a `fn`, and runs the function
  against once against this Transaction"
  [^TransactionContext tc ^IFn tc-fn]
  (.run tc (as-java-fn tc-fn)))

(defn set
  "Sets the value for a given key."
  [^Transaction tr ^"[B" k ^"[B" v]
  (.set tr k v))

(defn get
  "Gets a value from the database. The call will return null if the
  key is not present in the database."
  [^Transaction tr ^"[B" k]
  (.get tr k))

(defn clear-key
  "When given a Transaction and a key, clears a given key from the
  database. This will not affect the database until commit() is
  called."
  [^Transaction tr ^"[B" k]
  (.clear tr k))


(defn clear-range
  "When given a Range, clears a range of keys in the database. The
  upper bound of the range is exclusive; that is, the key (if one
  exists) that is specified as the end of the range will NOT be
  cleared as part of this operation. Range clears are efficient with
  FoundationDB -- clearing large amounts of data will be fast. This
  will not affect the database until commit() is called."
  [^Transaction tr ^Range rg]
  (.clear tr rg))
