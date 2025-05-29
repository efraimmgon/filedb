(ns filedb.core
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string]
   [clojure.test :as test]
   [filedb.protocols :as p]
   [me.raynes.fs :as fs]))

(def counter-doc-id
  "Filename for the document that stores the next available ID for a collection.
   Default value: \"__counter__.edn\"")

(defn read-string*
  "Reads an EDN string with support for #inst literals representing java.time.Instant.
   Parameters:
   - s: A string containing EDN data.
   Returns:
   - The parsed EDN data structure.
   Example:
   (read-string* \"#inst \\\"2024-03-20T10:00:00Z\\\"\")
   ;=> #object[java.time.Instant \"2024-03-20T10:00:00Z\"]"
  [s]
  (edn/read-string
   {:readers {'inst #(java.time.Instant/parse %)}}
   s))

;;; ----------------------------------------------------------------------------
;;; Utils
;;; ----------------------------------------------------------------------------
(defn- mkdirs-if-not-exist!
  "Ensures that the directory structure for a given collection exists.
   Creates directories under the database's root path if they are missing.
   `db` is the database instance map, expected to contain `:db-root`.
   `colln` is the raw collection identifier (scalar or vector).
   Returns a java.io.File object representing the directory."
  [db colln]
  (let [parsed (p/parse-coll-name colln)
        dir (io/file (:db-root db)
                     (if (vector? colln)
                       (clojure.string/join "/" parsed)
                       parsed))]
    (when-not (.exists dir)
      (.mkdirs dir))
    dir))

(defn ->as-file
  "Converts an entity into a file path.
   - Single arity: 
    for collections (e.g., (as-file db :users) -> filedb/users)
   - Double arity: 
    for documents (e.g., (as-file db :users \"123\") -> filedb/users/123)"
  ([db coll]
   (->as-file db coll nil))
  ([db colln doc-id]
   (if-not doc-id
     (io/file (mkdirs-if-not-exist! db colln))
     (io/file (mkdirs-if-not-exist! db colln) (str doc-id)))))

(defn mkvec
  "Ensures a value is wrapped in a vector.
   
   Parameters:
   - x: Any value
   
   Returns:
   - If x is already a vector, returns x unchanged
   - Otherwise, returns x wrapped in a vector
   
   Example:
   (mkvec :users)         ;=> [:users]
   (mkvec [:users :deps]) ;=> [:users :deps]"
  [x]
  (if-not (vector? x)
    [x]
    x))

;; So that java.time.Instant prints as a inst literal
(defmethod print-method java.time.Instant [obj ^java.io.Writer w]
  (.write w (str "#inst \"" (.toString obj) "\"")))

(defn now
  "Returns the current timestamp as a java.time.Instant.
   Used for setting created-at and updated-at fields.
   
   Returns:
   - Current time as java.time.Instant
   
   Example:
   (now) ;=> #inst \"2024-03-20T10:00:00.000Z\""
  []
  (java.time.Instant/now))

(defn maybe-add-created-at
  "Adds a created-at timestamp to a map if it doesn't already have one.
   Parameters:
   - data: The map to potentially add the timestamp to.
   - k: The keyword to use for the created-at timestamp.
   - timestamp: The timestamp value (java.time.Instant) to add.
   Returns:
   - The data map, possibly with the added created-at timestamp.
   Example:
   (maybe-add-created-at {} :created-at (now))
   ;=> {:created-at #inst \"2024-03-20T10:00:00.000Z\"}"
  [data k timestamp]
  (if (contains? data k)
    data
    (assoc data k timestamp)))

(defn add-updated-at
  "Adds or updates the updated-at timestamp in a map.
   Parameters:
   - data: The map to add/update the timestamp in.
   - k: The keyword to use for the updated-at timestamp.
   - timestamp: The timestamp value (java.time.Instant) to set.
   Returns:
   - The data map with the updated_at timestamp.
   Example:
   (add-updated-at {:updated-at (now)} :updated-at (now))
   ;=> {:updated-at #inst \"2024-03-20T10:00:01.000Z\"}"
  [data k timestamp]
  (assoc data k timestamp))

;;; ----------------------------------------------------------------------------
;;; Core
;;; ----------------------------------------------------------------------------

(defn limit-clause
  "Applies a limit to a sequence. If limit is 1, returns the first item only.
   Otherwise, returns a sequence of at most 'limit' items.
   If limit is nil or not provided, returns the original sequence.
   Parameters:
   - limit: The maximum number of items to return (integer or nil).
   - coll: The sequence to limit.
   Returns:
   - The first item if limit is 1, or a potentially limited sequence.
   Example:
   (limit-clause 1 [1 2 3])   ;=> 1
   (limit-clause 2 [1 2 3])   ;=> (1 2)
   (limit-clause nil [1 2 3]) ;=> (1 2 3)"
  [limit coll]
  (if (= limit 1)
    (first coll)
    (take limit coll)))

(defn kv=
  "Creates a predicate function that checks if a map contains a specific 
   key-value pair.
   Commonly used with the query function's :where clause.
   
   Parameters:
   - k: The key to check
   - v: The value to compare against
   
   Returns:
   - A function that takes a map and returns true if the map contains the 
   key-value pair
   
   Example:
   (def active-users (kv= :status :active))
   (active-users {:status :active})  ;=> true
   (active-users {:status :inactive}) ;=> false
   
   ;; Usage with query:
   (query :users {:where (kv= :role :admin)})"
  [k v]
  (fn [ctx]
    (= (get ctx k) v)))

(defn- build-params
  "Recursively builds a parameter map for the `query` function.
   Primarily used internally by `get-by-key` to construct the `:where` clause
   by AND-ing multiple key-value conditions, and merging other options.
   Parameters:
   - acc: Accumulator map for query parameters.
   - k: Key for a condition or an options map.
   - v: Value for a condition.
   - kvs-opts: Sequence of further key-value pairs or options maps.
   Returns:
   - A map of query parameters suitable for `p/query`."
  [acc k v kvs-opts]
  (cond
    (not (contains? acc :where))
    (build-params
     {:where (kv= k v)}
     (first kvs-opts)
     (second kvs-opts)
     (rest (rest kvs-opts)))

    (and k v)
    (build-params
     (update acc :where
             (fn [f]
               (fn [ctx]
                 (and
                  (f ctx)
                  ((kv= k v) ctx)))))
     (first kvs-opts)
     (second kvs-opts)
     (rest (rest kvs-opts)))

    k
    (merge acc k)

    :else
    acc))

(defrecord FileDB [db-root keyword-strategy]
  p/IFileDB

  (maybe-add-timestamps [_this coll data]
    (let [timestamp (now)]
      (-> data
          (maybe-add-created-at
           (p/created-at-keyword keyword-strategy coll) timestamp)
          (add-updated-at
           (p/updated-at-keyword keyword-strategy coll) timestamp))))

  (get-config-path [this coll]
    (->as-file
     this, (into ["__fsdb__"] (mkvec coll))))

  (get-next-id [this coll]
    (let [counter-file
          (->as-file this, (into ["__fsdb__"] (mkvec coll)), counter-doc-id)]
      (if (.exists counter-file)
        (let [current-id (read-string* (slurp counter-file))]
          (spit counter-file (str (inc current-id)))
          (inc current-id))
        (let [next-id 1]
          (spit counter-file next-id)
          next-id))))

  (get-by-id [this coll id]
    (when id
      (let [entry-file (->as-file this coll (str id))]
        (newline)
        (when (.exists entry-file)
          (read-string* (slurp entry-file))))))

  (get-by-ids [this coll ids]
    (let [table-path (->as-file this coll)]
      (->> ids
           (map str)
           (map #(io/file table-path %))
           (filter #(.exists %))
           (map #(read-string* (slurp %))))))

  (get-all [this coll]
    (let [table-path (->as-file this coll)]
      (->> (.listFiles table-path)
           (filter #(.isFile %))
           (map #(read-string* (slurp %))))))

  (query [this coll {:keys [limit offset order-by where]}]
    (let [result
          (cond->> (p/get-all this coll)
            where (filter where)
            order-by (sort-by (first order-by)
                              (case (second order-by)
                                :asc compare
                                :desc #(compare %2 %1)))
            offset (drop offset)
            limit (limit-clause limit))]
      result))

  (get-by-key [this coll k v]
    (p/get-by-key this coll k v nil))

  (get-by-key [this coll k v kv-opts]
    (p/query this
             coll
             (build-params {} k v kv-opts)))

  (insert! [this coll data]
    (let [id-kw (p/id-keyword keyword-strategy coll)
          id (if-let [id (id-kw data)]
               id
               (p/get-next-id this coll))
          data-with-id (p/maybe-add-timestamps this
                                               coll
                                               (assoc data id-kw id))
          entry-file (->as-file this coll (str id))]
      (spit entry-file (pr-str data-with-id))
      data-with-id))

  (update! [this coll id data-or-fn]
    (let [entry-file (->as-file this coll (str id))]
      (if (.exists entry-file)
        (let [existing-data (read-string* (slurp entry-file))

              updated
              (if (test/function? data-or-fn)
                (data-or-fn existing-data)
                (merge existing-data data-or-fn))

              updated-data (p/maybe-add-timestamps this coll updated)]
          (spit entry-file (pr-str updated-data))
          updated-data)
        false)))

  (delete! [this coll id]
    (let [entry-file (->as-file this coll id)]
      (if (.exists entry-file)
        (do
          (.delete entry-file)
          true)
        false)))

  (reset-db! [_this]
    (fs/delete-dir db-root))

  (delete-coll! [this coll]
    (->> coll (p/get-config-path this) fs/delete-dir)
    (let [file (->as-file this coll)]
      (when (.exists file)
        (-> (fs/delete-dir file)))))

  (get-count [this coll]
    (->> (->as-file this coll)
         (fs/list-dir)
         (filter (fn [x]
                   (.isFile x)))
         (count))))

(def default-db
  "The default FileDB instance.
   Uses \"filedb\" as the root directory and `p/default-keywords` for keyword strategy."
  (->FileDB
   "filedb"
   p/default-keywords))
