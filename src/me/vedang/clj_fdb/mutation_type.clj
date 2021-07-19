(ns me.vedang.clj-fdb.mutation-type
  "A set of operations that can be performed atomically on a database.
  These are used as parameters to `ftr/mutate!` (mutation, byte[],
  byte[])."
  (:import com.apple.foundationdb.MutationType))


(def mutation-types
  "Mutation types. This corresponds to `MutationType` enum."
  {:add MutationType/ADD
   :append-if-fits MutationType/APPEND_IF_FITS
   :bit-and MutationType/BIT_AND
   :bit-or MutationType/BIT_OR
   :bit-xor MutationType/BIT_XOR
   :byte-max MutationType/BYTE_MAX
   :byte-min MutationType/BYTE_MIN
   :compare-and-clear MutationType/COMPARE_AND_CLEAR
   :max MutationType/MAX
   :min MutationType/MIN
   :set-versionstamped-key MutationType/SET_VERSIONSTAMPED_KEY
   :set-versionstamped-value MutationType/SET_VERSIONSTAMPED_VALUE})


(defn mutation
  "Given a mutation-type `k`, return the corresponding value from the
  `MutationType` enum."
  ;; This is a helper function to access types from the map, throws
  ;; asserts on bad input
  [k]
  {:pre [(keyword? k)]
   :post [some?]}
  (mutation-types k))
