(ns me.vedang.clj-fdb.tuple-io
  (:refer-clojure :exclude [get set])
  (:require
   [byte-streams :as bs]

   [me.vedang.clj-fdb.subspace.subspace :as fsubspace]
   [me.vedang.clj-fdb.transaction :as ftr])
  (:use [me.vedang.clj-fdb.tuple.tuple :only [pack]])
  (:import
   [com.apple.foundationdb
    KeyValue
    Range
    Transaction
    TransactionContext]
   [com.apple.foundationdb.subspace  Subspace]
   [com.apple.foundationdb.tuple  Tuple]
   [java.lang   IllegalArgumentException]))


(defn set
  "Takes the following:
  - TransactionContext `tc`
  - key to be stored `k`. Must be of type Tuple
  - value to be stored `v` Must be of type Tuple

  and stores `v` against `k` in FDB. Returns nil.

  "
  [^TransactionContext tc ^Tuple k ^bytes v ] 
  ;(println "setter called , bytes " (bytes? v) ", tuple :" (class k) ) 
  (let [tr-fn (fn [^Transaction tr]
                (ftr/set tr (pack k) v))]
    (ftr/run tc tr-fn)))


(defn get-future
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k`

  and returns bytes `v` against `k` in FDB.

"
  
  [^TransactionContext tc ^Tuple k]
  (let [tr-fn (fn [^Transaction tr] (ftr/get tr (pack k)) )]
    (ftr/run tc tr-fn)))
  
     
(defn get
  "Takes the following:
  - TransactionContext `tc`
  - key to be fetched `k`

  and returns bytes `v` against `k` in FDB.

"

  [^TransactionContext tc ^Tuple k]
  (deref (get-future tc k))
  )



(defn get-range
  "Takes the following:
  - TransactionContext `tc`
  - Range of keys to fetch `rg`

  and returns a list of key/value pairs (list [^Tuple key ^bytes val] ..)."
  [^TransactionContext tc ^Range rg ]
  (let [tr-fn (fn [^Transaction tr]
                (reduce (fn [v  ^KeyValue kv] (conj v [(.getKey kv) (.getValue kv)])) [] (ftr/get-range tr rg)) )]
    (ftr/run tc tr-fn)))

