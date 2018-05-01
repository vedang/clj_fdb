(ns clj-fdb.byte-conversions
  (:require [byte-streams :as bs]
            [clj-fdb.tuple :as ftup])
  (:import byte_streams.Utils
           com.apple.foundationdb.tuple.Tuple
           java.nio.ByteBuffer))

(def ^:private ^:const byte-array (class (Utils/byteArray 0)))

(bs/def-conversion [com.apple.foundationdb.tuple.Tuple byte-array]
  [t]
  (ftup/pack ^Tuple t))

(bs/def-conversion [byte-array com.apple.foundationdb.tuple.Tuple]
  [ba]
  (ftup/from-bytes ^"[B" ba))

(bs/def-conversion [java.lang.Short byte-array]
  [short-int]
  (.. (ByteBuffer/allocate 2)
      (putShort short-int)
      array))

(bs/def-conversion [byte-array java.lang.Short]
  [short-int-ba]
  (.. (ByteBuffer/wrap short-int-ba)
      (getShort)))

(bs/def-conversion [java.lang.Integer byte-array]
  [integer]
  (.. (ByteBuffer/allocate 4)
      (putInt integer)
      array))

(bs/def-conversion [byte-array java.lang.Integer]
  [integer-ba]
  (.. (ByteBuffer/wrap integer-ba)
      (getInt)))

(bs/def-conversion [java.lang.Long byte-array]
  [long-int]
  (.. (ByteBuffer/allocate 8)
      (putLong long-int)
      array))

(bs/def-conversion [byte-array java.lang.Long]
  [long-int-ba]
  (.. (ByteBuffer/wrap long-int-ba)
      (getLong)))
