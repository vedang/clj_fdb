(ns me.vedang.clj-fdb.FDB
  (:import
    (com.apple.foundationdb
      FDB
      FDBDatabase
      NetworkOptions)
    java.util.concurrent.Executor))


(def clj-fdb-api-version
  "The API version used by this library in all tests and documentation"
  620)


(defn ^NetworkOptions options
  "Returns a set of options that can be set on a the FoundationDB API.
  Generally, these options to the top level of the API affect the
  networking engine and therefore must be set before the network
  engine is started. The network is started by calls to startNetwork()
  and implicitly by calls to open() and createCluster() (and their
  respective variants)."
  [^FDB db]
  (.options db))


(defn ^FDBDatabase select-api-version
  "Select the version for the client API. An exception will be thrown
  if the requested version is not supported by this implementation of
  the API. As only one version can be selected for the lifetime of the
  JVM, the result of a successful call to this method is always the
  same instance of a FDB object.

  Warning: When using the multi-version client API, setting an API
  version that is not supported by a particular client library will
  prevent that client from being used to connect to the cluster. In
  particular, you should not advance the API version of your
  application after upgrading your client until the cluster has also
  been upgraded."
  [^Integer version]
  (FDB/selectAPIVersion version))


(defn set-unclosed-warning
  "Enables or disables the stderr warning that is printed whenever an
  object with FoundationDB native resources is garbage collected
  without being closed. By default, this feature is enabled."
  [^FDB db warn-on-unclosed?]
  (.setUnclosedWarning db warn-on-unclosed?))


(defn open
  "Initializes networking, connects with the default fdb.cluster file,
  and opens the database.

  Returns:
  - a `CompletableFuture` that will be set to a FoundationDB Database

  Throws: `FDBException`"
  ([^FDB db]
   (.open db))
  ([^FDB db cluster-file-path]
   (.open db cluster-file-path))
  ([^FDB db cluster-file-path ^Executor e]
   (.open db cluster-file-path e)))


(defn start-network
  "Initializes networking. Can only be called once. This version of
  startNetwork() will create a new thread and execute the networking
  event loop on that thread. This method is called upon Database or
  Cluster creation by default if the network has not yet been started.
  If one wishes to control what thread the network runs on, one should
  use the version of startNetwork() that takes an Executor.

  Configuration of the networking engine can be achieved through calls
  to the methods in NetworkOptions.

  Throws:
  - java.lang.IllegalStateException - if the network has already been
  stopped
  - FDBException

  See Also: NetworkOptions."
  ([^FDB db]
   (.startNetwork db))
  ([^FDB db ^Executor e]
   (.startNetwork db e)))


(defn stop-network
  "Stops the FoundationDB networking engine. This can be called only
  once -- the network cannot be restarted after this call. This call
  blocks for the completion of the FoundationDB networking engine.

  Throws:
  - FDBException - on errors while stopping the network."
  [^FDB db]
  (.stopNetwork db))
