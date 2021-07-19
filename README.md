# clj-fdb
[![Circle CI](https://circleci.com/gh/vedang/clj_fdb.svg?style=svg)](https://app.circleci.com/pipelines/github/vedang/clj_fdb)
[![Clojars Project](https://img.shields.io/clojars/v/me.vedang/clj-fdb.svg)](https://clojars.org/me.vedang/clj-fdb)

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

FDB only stores data as bytes. When using this library, you are
expected to pass in data (both keys as well as values) as either:
- Byte Arrays
- Strings (converted to byte-arrays internally with a UTF-8 encoding)
- FDB data-structures (Tuples, Subspaces, DirectoryLayers, converted
  to byte-arrays internally using functions provided by FDB)

The library transparently converts Tuples, Subspaces and
DirectoryLayers into byte-arrays (and back) so my recommendation is
that you always use these for keys in FDB, and where possible use
Tuples to wrap the values you want to store in FDB. If you build your
keys and values using Clojure's `vector` function, your data will be
handled automatically. Note that if you use non-Tuple data as values,
the library will return the byte-array as-is to you. You need to
either pass in a `:valfn` to convert the byte-array to data or you
need to handle the conversion yourself. Refer to the examples below.

The idea is to write a really thin "clojure-y" wrapper on top of the
Java API. The `core.clj` file provides wrapped functions that make
using the API simpler, but you should be able to drop down when you
need to. I've chosen to mimic the directory structure of the
underlying Java driver. So the style is as follows:

    - `src/me/vedang/clj_fdb/` mimics `com.apple.foundationdb` (with
      `transaction.clj` and `FDB.clj`)
    - `src/me/vedang/clj_fdb/tuple/` mimics `com.apple.foundationdb.tuple` (with
      `tuple.clj`)

... and so on. The complete Java API is not available at the moment,
and will be built out as per my requirements (or via PRs, please).
Currently, the core namespace provides sync functions for working with
to Raw KV, Tuples, Subspaces and Directories.

Going through `transaction.clj` or `tuple.clj` or `FDB.clj` will give
you a clear idea of what I have in mind, please help me by
contributing PRs!

The complete documentation is available at:
https://cljdoc.org/d/me.vedang/clj-fdb/0.3.0

## Installation

* Use the library in your Clojure projects by adding the dep in
  `project.clj`
```
[me.vedang/clj-fdb "0.3.0"]
```

## Examples

Here is some test code to demonstrate how to use the functions defined
in the core ns:
```clojure
;; To run this code, you will need to require the following in your project:
;; [me.vedang/clj-fdb "0.3.0"]
(comment
  (require '[me.vedang.clj-fdb.FDB :as cfdb]
           '[me.vedang.clj-fdb.core :as fc]
           '[me.vedang.clj-fdb.transaction :as ftr]
           '[me.vedang.clj-fdb.tuple.tuple :as ftup]
           '[me.vedang.clj-fdb.subspace.subspace :as fsub])

  ;; Set a value in the DB.
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (fc/set db ["a" "test" "key"] ["some value"])))
  ;; => nil

  ;; Read this value back in the DB.
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (fc/get db ["a" "test" "key"])))
  ;; => ["some value"]

  ;; FDB's Tuple Layer is super handy for efficient range reads. Each
  ;; element of the tuple can act as a prefix (from left to right).
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (fc/set db ["test" "keys" "A"] ["value A"])
      (fc/set db ["test" "keys" "B"] ["value B"])
      (fc/set db ["test" "keys" "C"] ["value C"])
      (fc/get-range db ["test" "keys"])))
  ;; => {["test" "keys" "A"] ["value A"],
  ;;     ["test" "keys" "B"] ["value B"],
  ;;     ["test" "keys" "C"] ["value C"]}

  ;; FDB's Subspace Layer provides a neat way to logically namespace keys.
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        subspace (fsub/create ["test" "keys"])]
    (with-open [db (cfdb/open fdb)]
      (fc/set db subspace ["A"] ["Value A"])
      (fc/set db subspace ["B"] ["Value B"])
      (fc/get db subspace ["A"])))
  ;; => ["Value A"]

  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        subspace (fsub/create ["test" "keys"])]
    (with-open [db (cfdb/open fdb)]
      (fc/set db subspace ["A"] ["Value A"])
      (fc/set db subspace ["B"] ["Value B"])
      (fc/get-range db subspace [] {:valfn first})))
  ;; => {["A"] "Value A", ["B"] "Value B"}

  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
        subspace (fsub/create ["test" "keys"])]
    (with-open [db (cfdb/open fdb)]
      (fc/set db subspace ["A"] ["Value A"])
      (fc/set db subspace ["B"] ["Value B"])
      (fc/get-range db subspace [])))
  ;; => {["A"] ["Value A"], ["B"] ["Value B"], ["C"] ["value C"]}

  ;; FDB's functions are beautifully composable. So you needn't
  ;; execute each step of the above function in independent
  ;; transactions. You can perform them all inside a single
  ;; transaction. (with the full power of ACID behind you)
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (ftr/run db
        (fn [tr]
          (fc/set tr ["test" "keys" "A"] ["value inside transaction A"])
          (fc/set tr ["test" "keys" "B"] ["value inside transaction B"])
          (fc/set tr ["test" "keys" "C"] ["value inside transaction C"])
          (fc/get-range tr ["test" "keys"])))))
  ;; => {["test" "keys" "A"] ["value inside transaction A"],
  ;;     ["test" "keys" "B"] ["value inside transaction B"],
  ;;     ["test" "keys" "C"] ["value inside transaction C"]}

  ;; The beauty and power of this is here:
  (let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
    (with-open [db (cfdb/open fdb)]
      (try (ftr/run db
             (fn [tr]
               (fc/set tr ["test" "keys" "A"] ["NEW value A"])
               (fc/set tr ["test" "keys" "B"] ["NEW value B"])
               (fc/set tr ["test" "keys" "C"] ["NEW value C"])
               (throw (ex-info "I don't like completing transactions"
                               {:boo :hoo}))))
           (catch Exception _
             (fc/get-range db ["test" "keys"])))))
  ;; => {["test" "keys" "A"] ["value inside transaction A"],
  ;;     ["test" "keys" "B"] ["value inside transaction B"],
  ;;     ["test" "keys" "C"] ["value inside transaction C"]}
  ;; No change to the values because the transaction did not succeed!

  ;; I hope this helps you get started with using this library!
)
```

I started writing this code in order to write the example
that FoundationDB has documented here:
https://apple.github.io/foundationdb/class-scheduling-java.html

This library has taken shape as a side-effect of trying to write that
example in Clojure.

You can find the Class Scheduler example
([here](https://github.com/vedang/farstar/blob/master/src/farstar/class_scheduling.clj)).

You can also find other examples of using the library
([here](https://github.com/vedang/farstar)).

Best of luck, and feedback welcome!

## Thanks

Thank you to Jan Rychter for feedback and discussion on shaping this library.
