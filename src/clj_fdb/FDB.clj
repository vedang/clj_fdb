(ns clj-fdb.FDB
  (:import com.apple.foundationdb.FDB))

(defn select-api-version
  "Select the version for the client API."
  [^Integer version]
  (FDB/selectAPIVersion version))

(defn open
  "Initializes networking, connects with the default fdb.cluster file,
  and opens the database."
  [^FDB db]
  (.open db))

(defn close
  "Close the Database object and release any associated resources."
  [^FDB db]
  (.close db))
