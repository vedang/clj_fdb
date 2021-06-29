(ns me.vedang.clj-fdb.transaction
  (:refer-clojure :exclude [get set])
  (:import clojure.lang.IFn
           [com.apple.foundationdb Range Transaction TransactionContext]
           com.apple.foundationdb.async.AsyncIterable
           java.util.concurrent.CompletableFuture
           [java.util.function Function Supplier]))

(defn- as-function
  "Takes a clojure fn and returns a `reify`'d version which implements
  `java.util.function.Function`.

  Note: The fn should accept a single argument."
  [f]
  (reify Function
    (apply [_this arg] (f arg))))


(defn- as-supplier
  "Takes a clojure fn and returns a `reify`'d version which implements
  `java.util.function.Supplier`.

  Note: The fn should accept 0 arguments"
  [f]
  (reify Supplier
    (get [_this] (f))))


(defn run
  "Takes a `TransactionContext` and a `fn`, and runs the function once
  against this Transaction. The call blocks while user code is
  executing, returning the result of that code on completion."
  [^TransactionContext tc ^IFn tr-fn]
  (.run tc (as-function tr-fn)))


(defn ^CompletableFuture run-async!
  "Takes a `TransactionContext` and a `fn`. Depending on the type of
  context, this may execute the supplied function multiple times if an
  error is encountered. This call is non-blocking -- control flow will
  return immediately with a `CompletableFuture` that will be set when
  the process is complete."
  [^TransactionContext tc ^IFn tr-fn]
  (.runAsync tc
             (as-function
              (fn [tr]
                (. CompletableFuture
                   (supplyAsync (as-supplier (fn [] (tr-fn tr)))))))))
(defn set
  "Sets the value for a given key."
  [^Transaction tr ^"[B" k ^"[B" v]
  (.set tr k v))


(defn ^CompletableFuture get
  "Gets a value from the database. The call will return null if the
  key is not present in the database."
  [^Transaction tr ^"[B" k]
  (.get tr k))


(defn ^AsyncIterable get-range
  "Gets an ordered range of keys and values from the database. The
  begin and end keys are specified by byte[] arrays, with the begin
  key inclusive and the end key exclusive. Ranges are returned from
  calls to Tuple.range() and Range.startsWith(byte[])."
  [^Transaction tr ^Range rg]
  (.getRange tr rg))


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
