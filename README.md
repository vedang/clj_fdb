# clj-fdb [![Build Status](https://travis-ci.org/vedang/clj_fdb.svg?branch=master)](https://travis-ci.org/vedang/clj_fdb)

A thin wrapper for the Java API for FoundationDB.

## Layout

To get started, you need to read/use the functions defined in
[src/me/vedang/clj_fdb/core.clj](https://github.com/vedang/clj_fdb/blob/master/src/me/vedang/clj_fdb/core.clj).
The impatient reader can jump to the [Examples section](#examples) to
see the functions in action.

At the moment, this ns provides the following functions:

    - set
    - get
    - clear
    - get-range
    - clear-range
    - set-subspaced-key
    - get-subspaced-key
    - clear-subspaced-key
    - get-subspaced-range
    - clear-subspaced-range

Since FDB only stores data as bytes, these functions will use the
[byte-streams](https://github.com/ztellman/byte-streams) library to
try and convert the input to byte-arrays. You can also pass your own
custom functions to convert data to/from byte-arrays.

The idea is to write a really thin "clojure-y" wrapper on top of the
Java API. The `core.clj` file provides wrapped functions that make
using the API simpler, but you should be able to drop down when you
need to. I've chosen to mimic the directory structure of the
underlying Java driver. So the style is as follows:

    - `src/me/vedang/clj_fdb/` mimics `com.apple.foundationdb` (with
      `transaction.clj` and `FDB.clj`)
    - `src/me/vedang/clj_fdb/tuple/` mimics `com.apple.foundationdb.tuple` (with
      `tuple.clj`)

... and so on. I haven't gotten around to actually writing the other
parts of the Java API at the moment. Going through `transaction.clj`
or `tuple.clj` or `FDB.clj` will give you a clear idea of what I have
in mind, please help me by contributing PRs!

The complete documentation is available at:
https://vedang.github.io/clj_fdb/

## Installation

* Use the library in your Clojure projects by adding the dep in
  `project.clj`
```
[me.vedang/clj-fdb "0.1.0"]
```

## Examples

Here is some test code to demonstrate how to use the functions defined
in the core ns:
```clojure
(comment
  (require '[byte-streams :as bs]
           '[me.vedang.clj-fdb.FDB :as cfdb]
           '[me.vedang.clj-fdb.core :as fc]
           '[me.vedang.clj-fdb.transaction :as ftr]
           '[me.vedang.clj-fdb.tuple.tuple :as ftup]
           '[me.vedang.clj-fdb.subspace.subspace :as fsubspace])

  ;; Set a value in the DB.
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (fc/set db "a:test:key" "some value")))
  ;; => nil

  ;; Read this value back in the DB.
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (fc/get db "a:test:key" :valfn bs/to-string)))
  ;; => "some value"

  ;; FDB's Tuple Layer is super handy for efficient range reads. Each
  ;; element of the tuple can act as a prefix (from left to right).
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (fc/set db (ftup/from "test" "keys" "A") "value A")
      (fc/set db (ftup/from "test" "keys" "B") "value B")
      (fc/set db (ftup/from "test" "keys" "C") "value C")
      (fc/get-range db
                    (ftup/range (ftup/from "test" "keys"))
                    :keyfn (comp ftup/get-items ftup/from-bytes)
                    :valfn bs/to-string)))
  ;; => {["test" "keys" "A"] "value A",
  ;;     ["test" "keys" "B"] "value B",
  ;;     ["test" "keys" "C"] "value C"}

  ;; FDB's Subspace Layer provides a neat way to logically namespace keys.
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        subspace (fsubspace/create-subspace (ftup/from "test" "keys"))]
    (with-open [db (cfdb/open fdb)]
      (fc/set-subspaced-key db subspace (ftup/from "A") "Value A")
      (fc/set-subspaced-key db subspace (ftup/from "B") "Value B")
      (fc/get-subspaced-key db subspace (ftup/from "A")
                            :valfn #(bs/convert % String))
      (fc/get-subspaced-range db subspace (ftup/from)
                              :keyfn (comp ftup/get-items ftup/from-bytes)
                              :valfn #(bs/convert % String))))
  ;; => {["test" "keys" "A"] "Value A", ["test" "keys" "B"] "Value B"}

  ;; FDB's functions are beautifully composable. So you needn't
  ;; execute each step of the above function in independent
  ;; transactions. You can perform them all inside a single
  ;; transaction. (with the full power of ACID behind you)
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (ftr/run db
        (fn [tr]
          (fc/set tr (ftup/from "test" "keys" "A") "value inside transaction A")
          (fc/set tr (ftup/from "test" "keys" "B") "value inside transaction B")
          (fc/set tr (ftup/from "test" "keys" "C") "value inside transaction C")
          (fc/get-range tr
                        (ftup/range (ftup/from "test" "keys"))
                        :keyfn (comp ftup/get-items ftup/from-bytes)
                        :valfn bs/to-string)))))
  ;; => {["test" "keys" "A"] "value inside transaction A",
  ;;     ["test" "keys" "B"] "value inside transaction B",
  ;;     ["test" "keys" "C"] "value inside transaction C"}

  ;; The beauty and power of this is here:
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (try (ftr/run db
             (fn [tr]
               (fc/set tr (ftup/from "test" "keys" "A") "NEW value A")
               (fc/set tr (ftup/from "test" "keys" "B") "NEW value B")
               (fc/set tr (ftup/from "test" "keys" "C") "NEW value C")
               (throw (ex-info "I don't like completing transactions"
                               {:boo :hoo}))))
           (catch Exception _
             (fc/get-range db
                           (ftup/range (ftup/from "test" "keys"))
                           :keyfn (comp ftup/get-items ftup/from-bytes)
                           :valfn bs/to-string)))))
  ;; => {["test" "keys" "A"] "value inside transaction A",
  ;;     ["test" "keys" "B"] "value inside transaction B",
  ;;     ["test" "keys" "C"] "value inside transaction C"}
  ;; No change to the values because the transaction did not succeed!

  ;; I hope this helps you get started with using this library!
)
```

I started writing this code in order to write the example
that FoundationDB has documented here:
https://apple.github.io/foundationdb/class-scheduling-java.html

This library has taken shape as a side-effect of trying to write that
example in Clojure.

You can find the Class Scheduler example in the top-level `examples/`
folder
([here](https://github.com/vedang/clj_fdb/blob/master/src/examples/class_scheduling.clj)).
This gives the reader a good idea of how to use `clj-fdb`. Refer to
the comment block at the end of the example for how to run the
example.
