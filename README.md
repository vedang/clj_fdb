# clj-fdb

A thin wrapper for the Java API for FoundationDB.

## Layout

To get started, you need to read/use the functions defined in
`src/clj_fdb/core.clj`. At the moment, this ns provides the following
functions:

    - set
    - get
    - clear
    - get-range
    - clear-range

Since FDB only stores data as bytes, these functions will use the
[byte-streams](https://github.com/ztellman/byte-streams) library to
try and convert the input to byte-arrays. You can also pass your own
custom functions to convert data to/from byte-arrays.

The idea is to write a really thin "clojure-y" wrapper on top of the
Java API. The `core.clj` file provides wrapped functions that make
using the API simpler, but you should be able to drop down when you
need to. I've chosen to mimic the directory structure of the
underlying Java driver. So the style is as follows:

    - `src/clj_fdb/` mimics `com.apple.foundationdb` (with
      `transaction.clj` and `FDB.clj`)
    - `src/clj_fdb/tuple/` mimics `com.apple.foundationdb.tuple` (with
      `tuple.clj`)

... and so on. I haven't gotten around to actually writing the other
parts of the Java API at the moment. Going through `transaction.clj`
or `tuple.clj` or `FDB.clj` will give you a clear idea of what I have
in mind, please help me by contributing PRs!

## Installation

Currently, installing this library from Clojars is broken. This is
because the FoundationDB Java API Jar is not available for download
from a central repository like Maven. To use this library, you need to
download and install it locally. You can do this as follows:

* Download the FoundationDB Java API Jar from here:
  https://www.foundationdb.org/downloads/5.1.7/bindings/java/fdb-java-5.1.7.jar
* Install it to your Maven repository as follows:
```
$ mvn install:install-file -Dfile=fdb-java-5.1.7.jar -DgroupId=com.apple.foundationdb -DartifactId=fdb-java -Dversion=5.1.7 -Dpackaging=jar
```
* Download this library from Github by cloning this project.
* Run the following commands in the top level of the library
```
$ lein uberjar
$ lein install
```
* Use the library in your Clojure projects by adding the dep in
  `project.clj`
```
[clj-fdb "0.1.0"]
```

## Examples

Actually, I started writing this code in order to write the example
that FoundationDB has documented here:
https://apple.github.io/foundationdb/class-scheduling-java.html

This library has taken shape as a side-effect of trying to write that
example in Clojure.

You can find the Class Scheduler example in the top-level `examples/`
folder. This gives the reader a good idea of how to use `clj-fdb`.
Refer to the comment block at the end of the example to see how to use
the library on the REPL.

## Other Notes

There are no tests at the moment. I do plan on writing them at some
point in time. Please contribute tests if you can!
