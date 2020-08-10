(ns me.vedang.clj-fdb.tuple.converters
  (:refer-clojure :exclude [range])
  (:import
   [java.io ByteArrayOutputStream DataOutputStream]
  
  [java.nio ByteOrder ByteBuffer]
  [com.apple.foundationdb.tuple  Tuple]))


(defn encode-long[^long l]
  (-> (ByteBuffer/allocate 8)
      (.order  ByteOrder/LITTLE_ENDIAN)
      (.putLong  l)
      (.array )
      ))

(defn decode-long [^bytes l-array]  
  (-> (ByteBuffer/allocate 8)
      (.order  ByteOrder/LITTLE_ENDIAN)
      (.put  l-array)
      (.getLong  0)
      ))





(defn encode-int [i]
  (-> (ByteBuffer/allocate 4) 
      (.putInt  i)
      (.array )
      ))

(defn decode-int [^bytes i-array]
  (->(ByteBuffer/allocate 4)
      (.put  i-array)
      (.getInt  0)
   ))

