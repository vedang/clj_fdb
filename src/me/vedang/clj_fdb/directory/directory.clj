(ns me.vedang.clj-fdb.directory.directory
  (:refer-clojure :exclude [list])
  (:import [com.apple.foundationdb.directory DirectoryLayer DirectorySubspace]
           com.apple.foundationdb.TransactionContext
           java.util.concurrent.CompletableFuture))

(def default-directory-layer (DirectoryLayer/getDefault))


(defn ^DirectorySubspace create-or-open!
  "Creates or opens the Directory located at `path` (creating parent
  directories, if necessary). Returns a `DirectorySubspace` on completion."
  ([^TransactionContext tr path]
   (create-or-open! tr default-directory-layer path))
  ([^TransactionContext tr ^DirectoryLayer dl path]
   (deref (.createOrOpen dl tr path))))


(defn ^Boolean exists?
  "Check if the directory exists at `path`"
  ([^TransactionContext tr path]
   (exists? tr default-directory-layer path))
  ([^TransactionContext tr ds path]
   (deref (.exists ^DirectoryLayer ds tr path))))


(defn ^Boolean list
  "List the subdirectories at the given `path`."
  ([tr]
   (list tr default-directory-layer))
  ([tr ds]
   (deref (.list ^DirectoryLayer ds tr))))


(defn ^CompletableFuture remove!
  "Removes this Directory and all of its subdirectories, as well as all
  of their contents."
  ;; Use with extreme caution :D
  ([^TransactionContext tr path]
   (remove! tr default-directory-layer path))
  ([^TransactionContext tr ^DirectoryLayer dl path]
   (.remove dl tr path)))
