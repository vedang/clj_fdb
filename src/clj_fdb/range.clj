(ns clj-fdb.range
  "A simple description of an exact range of keyspace, specified by a
  begin and end key. As with all FoundationDB APIs, begin is
  inclusive, end exclusive."
  (:refer-clojure :exclude [range])
  (:import com.apple.foundationdb.Range))

(defn range
  "Construct a new Range with an inclusive begin key and an exclusive end key."
  [^"[B" begin ^"[B" end]
  (Range. begin end))

(defn starts-with
  "Returns a Range that describes all possible keys that are prefixed
  with a specified key.

  Parameters:
    prefix - the key prefixing the range, must not be null
  Returns:
    the range of keys starting with prefix"
  [^"[B" prefix]
  (Range/startsWith prefix))
