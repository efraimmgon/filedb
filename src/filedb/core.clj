(ns filedb.core
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string]
   [clojure.test :as test]
   [me.raynes.fs :as fs]))

;;; ----------------------------------------------------------------------------
;;; Default names
;;; ----------------------------------------------------------------------------

(def counter-doc-id
  "Filename for the document that stores the next available ID for a collection.
   Default value: \"__counter__.edn\""
  "__counter__.edn")

(def default-db-root
  "The default root directory for the database.
   Used by the default FileDB instance.
   
   Default value: \"filedb\""
  "filedb")

;;; ----------------------------------------------------------------------------
;;; Protocols, Records
;;; ----------------------------------------------------------------------------

(defmulti normalize-coll-name
  "Parses a collection name into a string representation based on its type.
   Supports strings, longs, symbols, keywords, and vectors.
   
   Parameters:
   - colln: The collection name to parse (can be string, long, symbol, keyword, or vector)
   
   Returns:
   - String representation of the collection name
   
   Examples:
   (normalize-coll-name \"users\")     ;=> \"users\"
   (normalize-coll-name :users)        ;=> \"users\"
   (normalize-coll-name 'users)        ;=> \"users\"
   (normalize-coll-name 123)          ;=> \"123\""
  (fn [colln]
    (type colln)))

(defmethod normalize-coll-name :default [colln] (str colln))

(defmethod normalize-coll-name java.lang.String [colln] colln)

(defmethod normalize-coll-name clojure.lang.Symbol [colln] (name colln))

(defmethod normalize-coll-name clojure.lang.Keyword [colln] (name colln))

(defn mk-keyword
  "Creates a keyword, optionally qualifying it based on the collection namespace type.
   
   Parameters:
   - colln: Collection name or path vector (e.g., :users or [:users 1 :profiles])
   - k: Base keyword to qualify (e.g., :id)
   - coll-ns-type: Namespace strategy (:simple, :partial, or :full)
   
   Returns:
   - :simple: Returns k unmodified
   - :partial: Returns k qualified with last collection name
   - :full: Returns k qualified with dot-separated collection path
   
   Examples:
   (mk-keyword :users :id :simple) ;=> :id
   (mk-keyword :users :id :partial) ;=> :users/id
   (mk-keyword [:users 1 :profiles] :id :full) ;=> :users.profiles/id"
  [colln k coll-ns-type]
  (case coll-ns-type
    :simple k
    :partial (if-not (vector? colln)
               (keyword (normalize-coll-name colln) (name k))
               (keyword (-> colln last normalize-coll-name) (name k)))
    :full (if-not (vector? colln)
            (keyword (normalize-coll-name colln) (name k))
            (let [normalized (for [[i name-] (map-indexed vector colln)
                                   ;; drop the ids
                                   :when (even? i)]
                               (normalize-coll-name name-))]
              (keyword (clojure.string/join "." normalized)
                       (name k))))))

(defprotocol KeywordStrategy
  "Protocol for defining how database keywords (like IDs and timestamps) are generated.
   Allows customization of keyword formats and namespacing strategies."
  (id-keyword [this coll]
    "Generates the keyword to be used for IDs in the given collection.
     
     Parameters:
     - coll: The collection name or path vector
     
     Returns:
     - A keyword to be used as the ID field")
  (created-at-keyword [this coll]
    "Generates the keyword to be used for creation timestamps.
     
     Parameters:
     - coll: The collection name or path vector
     
     Returns:
     - A keyword to be used as the created-at field")
  (updated-at-keyword [this coll]
    "Generates the keyword to be used for update timestamps.
     
     Parameters:
     - coll: The collection name or path vector
     
     Returns:
     - A keyword to be used as the updated-at field"))

(defrecord KeywordStrat [id created-at updated-at coll-ns-type]
  KeywordStrategy
  (id-keyword [_ coll] (mk-keyword coll id coll-ns-type))
  (created-at-keyword [_ coll] (mk-keyword coll created-at coll-ns-type))
  (updated-at-keyword [_ coll] (mk-keyword coll updated-at coll-ns-type)))

(def default-keywords
  "Default implementation of KeywordStrategy.
   Uses `:id`, `:created-at`, and `:updated-at` as base keywords
   and enables qualified keywords with :partial namespacing (e.g., `:user/id`).
   
   Fields:
   - :id :id
   - :created-at :created-at
   - :updated-at :updated-at
   - :coll-ns-type :partial
   
   Example usage:
   (id-keyword default-keywords :users) ;=> :users/id
   (created-at-keyword default-keywords :users) ;=> :users/created-at"
  (map->KeywordStrat
   {:id :id
    :created-at :created-at
    :updated-at :updated-at
    :coll-ns-type :partial}))

(defprotocol IFileDB
  (maybe-add-timestamps [this coll data]
    "Adds created-at and updated-at timestamps to data if they don't exist.
        
        Parameters:
        - coll: The name of the coll (for determining timestamp field names)
        - data: The map to add timestamps to
        
        Returns:
        - The data map with added/updated timestamps
        
        Example:
        (maybe-add-timestamps :users {})
        ;=> {:created-at #inst \"2024-03-20T10:00:00.000Z\"
        ;    :updated-at #inst \"2024-03-20T10:00:00.000Z\"}")

  (get-config-path [this coll]
    "Constructs the path to a coll's configuration directory.
       Used for storing coll-specific metadata and settings.
       
       Parameters:
       - coll: String, keyword, or vector for nested colls
       
       Returns:
       - A java.io.File object representing the coll's config directory
       
       Example:
       (get-config-path :users)
       ;=> #object[java.io.File \"fsdb/__fsdb__/users\"]")

  (get-next-id [this coll]
    "Generates and returns the next available ID for a coll.
       Uses a counter file to maintain ID sequence.
       
       Parameters:
       - coll: String, keyword, or vector for nested colls
       
       Returns:
       - The next available integer ID
       
       Example:
       (get-next-id :users) ;=> 1  ; First call
       (get-next-id :users) ;=> 2  ; Second call")

  (get-by-id [this coll id]
    "Retrieves a single entry from the database by its ID.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       - id: The unique identifier of the entry
       
       Returns:
       - The entry as a map if found
       - nil if no entry exists with the given ID
       
       Example:
       (get-by-id :users 1)
       (get-by-id [:organizations :departments] 42)")

  (get-by-ids [this coll ids]
    "Retrieves multiple entries from the database by their IDs in a single 
       operation. More efficient than making multiple get-by-id calls.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       - ids: A collection of IDs to retrieve
       
       Returns:
       - A sequence of entries that match the provided IDs
       - Empty sequence if no matches found
       
       Example:
       (get-by-ids :users [1 2 3])
       (get-by-ids [:orgs :deps] [42 43])")

  (get-all [this coll]
    "Retrieves all entries from a coll.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       
       Returns:
       - A sequence of all entries in the coll
       - Empty sequence if the coll is empty
       
       Example:
       (get-all :users)
       (get-all [:organizations :departments])")

  (query [this coll opts]
    "Performs a query on the coll with support for filtering, ordering, and 
       pagination.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       - opts: A map of query options:
         - :where    - A predicate function for filtering entries
         - :order-by - A vector of [field direction], where direction is :asc or 
       :desc
         - :offset   - Number of entries to skip
         - :limit    - Maximum number of entries to return
       
       Returns:
       - A sequence of entries matching the query criteria
       - If limit is 1, returns a single entry instead of a sequence
       
       Examples:
       (query :users 
             {:where #(> (:age %) 21)
              :order-by [:created-at :desc]
              :limit 10
              :offset 0})
       
       (query :orders
             {:where (fn [order] (= (:status order) :pending))
              :order-by [:date :asc]})")

  (get-by-key
    [this coll k v]
    [this coll k v opts]
    "Takes a key-value pair and optional query options map.
     Returns the entries that match. Accepts the same options as query.")

  (insert! [this coll data]
    "Inserts a new entry into the coll with automatic ID generation and 
       timestamps.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       - data: A map containing the entry data
              If :_id key is present, uses that as ID instead of generating one
       
       Returns:
       - The inserted data with added :_id, :_created-at, and :_updated-at fields
       
       Example:
       (insert :users 
              {:name \"John Doe\"
               :email \"john@example.com\"})
       
       (insert :users 
              {:_id \"custom-id\"
               :name \"Jane Doe\"})")

  (update! [this coll id data-or-fn]
    "Updates an existing entry by merging new data or applying a transformation 
       function.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       - id: The unique identifier of the entry to update
       - data-or-fn: Either:
                    - A map to merge with existing data
                    - A function that takes the existing data and returns updated 
                    data
       
       Returns:
       - The updated entry data if successful
       - false if the entry doesn't exist
       
       Examples:
       (update! :users 1 {:status :inactive})
       
       (update! :users 1
             (fn [user] 
               (update user :login-count inc)))")

  (delete! [this coll id]
    "Removes an entry from the coll.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       - id: The unique identifier of the entry to delete
       
       Returns:
       - true if the entry was successfully deleted
       - false if the entry didn't exist
       
       Example:
       (delete :users 1)
       (delete [:orgs :departments] 42)")

  (reset-db! [this]
    "Completely resets the database by removing all colls and their data.
       Use with caution as this operation cannot be undone.
       
       Returns:
       - nil
       
       Example:
       (reset-db!)")

  (delete-coll! [this coll]
    "Removes a coll and all its data from the database.
       Use with caution as this operation cannot be undone.
       
       Parameters:
       - coll: The name of the coll to drop
       
       Returns:
       - nil
       
       Example:
       (delete-coll! :users)
       (delete-coll! [:organizations :departments])")

  (get-count [this coll]
    "Returns the total number of entries in a coll.
       
       Parameters:
       - coll: The name of the coll (string, keyword, or vector for nested 
       colls)
       
       Returns:
       - The number of entries in the coll (integer)
      
       Example:
       (get-count :users)
       (get-count [:organizations :departments])"))

;;; ----------------------------------------------------------------------------
;;; Utils
;;; ----------------------------------------------------------------------------

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

(defn- mkdirs-if-not-exist!
  "Ensures that the directory structure for a given collection exists.
   Creates directories under the database's root path if they are missing.
   `db` is the database instance map, expected to contain `:db-root`.
   `colln` is the raw collection identifier (scalar or vector).
   Returns a java.io.File object representing the directory."
  [db colln]
  (let [path-parts (if (vector? colln)
                     ;; For nested collections like [:a 1 :b], create path: a/1/b
                     (mapv normalize-coll-name colln)
                     [(normalize-coll-name colln)])
        dir (apply io/file (:db-root db) path-parts)]
    (when-not (fs/exists? dir)
      (fs/mkdirs dir))
    dir))

(defn ->as-file
  "Converts an entity into a file path.
   - Single arity: 
    for collections (e.g., (as-file db :users) -> filedb/users)
   - Double arity: 
    for documents (e.g., (as-file db :users \"123\") -> filedb/users/123)"
  ([db colln]
   (->as-file db colln nil))
  ([db colln doc-id]
   (if-not doc-id
     (mkdirs-if-not-exist! db colln)
     (let [base-dir (mkdirs-if-not-exist! db colln)
           doc-dir (io/file base-dir (normalize-coll-name doc-id))
           file (io/file doc-dir "data.edn")]
       file))))

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
   - A map of query parameters suitable for `query`."
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

(defrecord FileDB [db-root keyword-strategy primary-key-type]
  IFileDB

  (maybe-add-timestamps [_this coll data]
    (let [timestamp (now)]
      (-> data
          (maybe-add-created-at
           (created-at-keyword keyword-strategy coll) timestamp)
          (add-updated-at
           (updated-at-keyword keyword-strategy coll) timestamp))))

  (get-config-path [this coll]
    (->as-file
     this, (into ["__fsdb__"] (mkvec coll))))

  (get-next-id [this coll]
    (case primary-key-type
      :uuid (str (java.util.UUID/randomUUID))
      :int-auto-increment
      (let [config-dir (get-config-path this coll)
            counter-file (io/file config-dir counter-doc-id)]
        (fs/mkdirs config-dir)
        (if (.exists counter-file)
          (let [current-id (read-string* (slurp counter-file))]
            (spit counter-file (str (inc current-id)))
            (inc current-id))
          (let [next-id 1]
            (spit counter-file next-id)
            next-id)))))

  (get-by-id [this coll id]
    (when id
      (let [entry-file (->as-file this coll (normalize-coll-name id))]
        (newline)
        (when (.exists entry-file)
          (read-string* (slurp entry-file))))))

  (get-by-ids [this coll ids]
    (let [table-path (->as-file this coll)]
      (->> ids
           (map normalize-coll-name)
           (map #(io/file table-path % "data.edn"))
           (filter #(.exists %))
           (map #(read-string* (slurp %))))))

  (get-all [this coll]
    (let [table-path (->as-file this coll)]
      (->> (.listFiles table-path)
           (filter #(.isDirectory %))
           (map #(io/file % "data.edn"))
           (filter #(.exists %))
           (map #(read-string* (slurp %))))))

  (query [this coll {:keys [limit offset order-by where]}]
    (let [result
          (cond->> (get-all this coll)
            where (filter where)
            order-by (sort-by (first order-by)
                              (case (second order-by)
                                :asc compare
                                :desc #(compare %2 %1)))
            offset (drop offset)
            limit (limit-clause limit))]
      result))

  (get-by-key [this coll k v]
    (get-by-key this coll k v nil))

  (get-by-key [this coll k v kv-opts]
    (query this
           coll
           (build-params {} k v kv-opts)))

  (insert! [this coll data]
    (let [id-kw (id-keyword keyword-strategy coll)
          id (if-let [id (or (id-kw data) (:id data))]
               id
               (get-next-id this coll))
          data-with-id (maybe-add-timestamps this
                                             coll
                                             (assoc data id-kw id))
          entry-file (->as-file this coll (normalize-coll-name id))]
      ;; Ensure document directory exists before writing
      (fs/mkdirs (.getParentFile entry-file))
      (spit entry-file (pr-str data-with-id))
      data-with-id))

  (update! [this coll id data-or-fn]
    (let [entry-file (->as-file this coll (normalize-coll-name id))]
      (if (.exists entry-file)
        (let [existing-data (read-string* (slurp entry-file))

              updated
              (if (test/function? data-or-fn)
                (data-or-fn existing-data)
                (merge existing-data data-or-fn))

              updated-data (maybe-add-timestamps this coll updated)]
          ;; Ensure document directory exists before writing
          (fs/mkdirs (.getParentFile entry-file))
          (spit entry-file (pr-str updated-data))
          updated-data)
        false)))

  (delete! [this coll id]
    (let [entry-file (->as-file this coll (normalize-coll-name id))]
      (if (.exists entry-file)
        (do
          (fs/delete-dir (.getParentFile entry-file))
          true)
        false)))

  (reset-db! [_this]
    (fs/delete-dir db-root))

  (delete-coll! [this coll]
    (->> coll (get-config-path this) fs/delete-dir)
    (let [file (->as-file this coll)]
      (when (.exists file)
        (-> (fs/delete-dir file)))))

  (get-count [this coll]
    (->> (->as-file this coll)
         (fs/list-dir)
         (filter (fn [x]
                   (.isDirectory x)))
         (count))))

(defn create-db
  "Creates a new FileDB instance with custom configuration.
   
   Parameters:
   - opts (optional): A map of options:
     - :db-root: The root directory for the database (default: \"filedb\")
     - :keyword-strategy: Strategy for keyword generation (default: default-keywords)
     - :primary-key-type: Strategy for ID generation, :int-auto-increment or :uuid (default: :int-auto-increment)
   
   Returns:
   - A new FileDB instance
   
   Examples:
   ;; Create a DB with UUID IDs
   (create-db {:primary-key-type :uuid})
   
   ;; Create a DB with custom root and UUID IDs
   (create-db {:db-root \"my-db\"
              :primary-key-type :uuid})"
  ([]
   (create-db {}))
  ([{:keys [db-root keyword-strategy primary-key-type]
     :or {db-root default-db-root
          keyword-strategy default-keywords
          primary-key-type :int-auto-increment}}]

   (->FileDB db-root keyword-strategy primary-key-type)))

