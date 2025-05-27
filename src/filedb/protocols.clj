(ns filedb.protocols
  (:require
   [clojure.java.io :as io]
   [clojure.string]))

(def ^:dynamic *db-root*
  "The root directory for the database.
   Can be dynamically rebound to change the database location.
   
   Default value: \"filedb\"
   
   Example:
   (binding [*db-root* \"custom-db\"]
     (insert :users {...}))"
  "filedb")

(defn mkdirs-if-not-exist! [^java.io.File dir]
  (when-not (.exists dir)
    (.mkdirs dir)))

(defprotocol IFileEntity
  (as-file
    [collection]
    [collection doc-id]
    "Converts an entity into a file path.
     - Single arity: 
      for collections (e.g., (as-file :users) -> filedb/users)
     - Double arity: 
      for documents (e.g., (as-file :users \"123\") -> filedb/users/123)"))

(extend-protocol IFileEntity
  java.lang.String
  (as-file
    ([coll]
     (let [f (io/file *db-root* coll)]
       (mkdirs-if-not-exist! f)
       f))
    ([coll doc-id]
     (io/file (as-file coll) (str doc-id))))

  java.lang.Long
  (as-file
    ([coll]
     (let [f (io/file *db-root* (str coll))]
       (mkdirs-if-not-exist! f)
       f))
    ([coll doc-id]
     (io/file (as-file coll) (str doc-id))))

  clojure.lang.Symbol
  (as-file
    ([coll]
     (let [f (io/file *db-root* (name coll))]
       (mkdirs-if-not-exist! f)
       f))
    ([coll doc-id]
     (io/file (as-file coll) (str doc-id))))

  clojure.lang.Keyword
  (as-file
    ([coll]
     (let [f (io/file *db-root* (name coll))]
       (mkdirs-if-not-exist! f)
       f))
    ([coll doc-id]
     (io/file (as-file coll) (str doc-id))))

  clojure.lang.PersistentVector
  (as-file
    ([coll]
     (let [f (->> coll (map name) (apply io/file *db-root*))]
       (mkdirs-if-not-exist! f)
       f))
    ([coll doc-id]
     (io/file (as-file coll) (str doc-id)))))