(ns filedb.core
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :as test]
   [filedb.protocols :as p]
   [me.raynes.fs :as fs]))

(def counter-doc-id
  "__counter__.edn")

(defn read-string*
  "Reads an EDN string with support for #inst literals.
   
   Parameters:
   - s: A string containing EDN data
   
   Returns:
   - The parsed EDN data with proper instant handling
   
   Example:
   (read-string* \"#inst \\\"2024-03-20T10:00:00Z\\\"\")"
  [s]
  (edn/read-string
   {:readers {'inst #(java.time.Instant/parse %)}}
   s))

;;; ----------------------------------------------------------------------------
;;; Utils
;;; ----------------------------------------------------------------------------

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

(defn deep-merge
  "Recursively merges maps and concatenates vectors.
   
   Parameters:
   - a: First map
   - b: Second map
   
   Returns:
   - A new map containing the merged contents of both maps
   - For nested maps, merges recursively
   - For vectors, concatenates them
   - For other values, takes the value from b
   
   Example:
   (deep-merge {:a {:b 1} :c [1]}
              {:a {:d 2} :c [2]})
   ;=> {:a {:b 1 :d 2} :c [1 2]}"
  [a b]
  (merge-with (fn [x y]
                (cond (map? y) (deep-merge x y)
                      (vector? y) (concat x y)
                      :else y))
              a b))

(defn ->str
  "Converts a value to its string representation.
   Handles keywords and symbols by extracting their names.
   
   Parameters:
   - x: Value to convert (keyword, symbol, or any other type)
   
   Returns:
   - For keywords/symbols: their name without namespace
   - For other types: the value itself
   
   Example:
   (->str :users)     ;=> \"users\"
   (->str 'my-table)  ;=> \"my-table\"
   (->str \"string\") ;=> \"string\""
  [x]
  (cond
    (keyword? x) (name x)
    (symbol? x) (name x)
    :else x))

(defn- id-keyword
  "Returns the standard ID field keyword for a table.
   This is an internal function used to maintain consistent ID field naming.
   
   Parameters:
   - table-name: The name of the table (unused, kept for future extensibility)
   
   Returns:
   - The keyword :_id
   
   Example:
   (id-keyword :users) ;=> :_id"
  [_table-name]
  :_id)

(defn- created-at-keyword
  "Returns the standard created-at field keyword for a table.
   This is an internal function used to maintain consistent timestamp field 
   naming.
   
   Parameters:
   - table-name: The name of the table (unused, kept for future extensibility)
   
   Returns:
   - The keyword :_created-at
   
   Example:
   (created-at-keyword :users) ;=> :_created-at"
  [_table-name]
  :_created-at)

(defn- updated-at-keyword
  "Returns the standard updated-at field keyword for a table.
   This is an internal function used to maintain consistent timestamp field 
   naming.
   
   Parameters:
   - table-name: The name of the table (unused, kept for future extensibility)
   
   Returns:
   - The keyword :_updated-at
   
   Example:
   (updated-at-keyword :users) ;=> :_updated-at"
  [_table-name]
  :_updated-at)

(defn get-config-path
  "Constructs the path to a table's configuration directory.
   Used for storing table-specific metadata and settings.
   
   Parameters:
   - table-name: String, keyword, or vector for nested tables
   
   Returns:
   - A java.io.File object representing the table's config directory
   
   Example:
   (get-config-path :users)
   ;=> #object[java.io.File \"fsdb/__fsdb__/users\"]"
  [table-name]
  (p/as-file
   (into ["__fsdb__"] (mkvec table-name))))

(defn get-next-id
  "Generates and returns the next available ID for a table.
   Uses a counter file to maintain ID sequence.
   
   Parameters:
   - table-name: String, keyword, or vector for nested tables
   
   Returns:
   - The next available integer ID
   
   Example:
   (get-next-id :users) ;=> 1  ; First call
   (get-next-id :users) ;=> 2  ; Second call"
  [table-name]
  (let [counter-file (p/as-file (into ["__fsdb__"] (mkvec table-name))
                                counter-doc-id)]
    (if (.exists counter-file)
      (let [current-id (read-string* (slurp counter-file))]
        (spit counter-file (str (inc current-id)))
        (inc current-id))
      (let [next-id 1]
        (spit counter-file next-id)
        next-id))))

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
  "Adds a created-at timestamp to data if it doesn't already exist.
   
   Parameters:
   - data: The map to potentially add the timestamp to
   - k: The key to use for the timestamp
   - timestamp: The timestamp value to add
   
   Returns:
   - The data map, possibly with added timestamp
   
   Example:
   (maybe-add-created-at {} :_created-at (now))
   ;=> {:_created-at #inst \"2024-03-20T10:00:00.000Z\"}"
  [data k timestamp]
  (if (contains? data k)
    data
    (assoc data k timestamp)))

(defn add-updated-at
  "Adds or updates the updated-at timestamp in the data.
   
   Parameters:
   - data: The map to add/update the timestamp in
   - k: The key to use for the timestamp
   - timestamp: The timestamp value to set
   
   Returns:
   - The data map with updated timestamp
   
   Example:
   (add-updated-at {} :_updated-at (now))
   ;=> {:_updated-at #inst \"2024-03-20T10:00:00.000Z\"}"
  [data k timestamp]
  (assoc data k timestamp))

(defn maybe-add-timestamps
  "Adds created-at and updated-at timestamps to data if they don't exist.
   
   Parameters:
   - table-name: The name of the table (for determining timestamp field names)
   - data: The map to add timestamps to
   
   Returns:
   - The data map with added/updated timestamps
   
   Example:
   (maybe-add-timestamps :users {})
   ;=> {:_created-at #inst \"2024-03-20T10:00:00.000Z\"
   ;    :_updated-at #inst \"2024-03-20T10:00:00.000Z\"}"
  [table-name data]
  (let [timestamp (now)]
    (-> data
        (maybe-add-created-at
         (created-at-keyword table-name) timestamp)
        (add-updated-at
         (updated-at-keyword table-name) timestamp))))

;;; ----------------------------------------------------------------------------
;;; Core
;;; ----------------------------------------------------------------------------

(defn get-by-id
  "Retrieves a single entry from the database by its ID.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
   - id: The unique identifier of the entry
   
   Returns:
   - The entry as a map if found
   - nil if no entry exists with the given ID
   
   Example:
   (get-by-id :users 1)
   (get-by-id [:organizations :departments] 42)"
  [table-name id]
  (when id
    (let [entry-file (p/as-file table-name (str id))]
      (newline)
      (when (.exists entry-file)
        (read-string* (slurp entry-file))))))

(defn get-by-ids
  "Retrieves multiple entries from the database by their IDs in a single 
   operation. More efficient than making multiple get-by-id calls.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
   - ids: A collection of IDs to retrieve
   
   Returns:
   - A sequence of entries that match the provided IDs
   - Empty sequence if no matches found
   
   Example:
   (get-by-ids :users [1 2 3])
   (get-by-ids [:orgs :deps] [42 43])"
  [table-name ids]
  (let [table-path (p/as-file table-name)]
    (->> ids
         (map str)
         (map #(io/file table-path %))
         (filter #(.exists %))
         (map #(read-string* (slurp %))))))

(defn get-all
  "Retrieves all entries from a table.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
   
   Returns:
   - A sequence of all entries in the table
   - Empty sequence if the table is empty
   
   Example:
   (get-all :users)
   (get-all [:organizations :departments])"
  [table-name]
  (let [table-path (p/as-file table-name)]
    (->> (.listFiles table-path)
         (filter #(.isFile %))
         (map #(read-string* (slurp %))))))

(defn limit-clause
  "Applies a limit to a collection, returning either the first element or a 
   limited sequence.
   
   Parameters:
   - limit: The maximum number of items to return
   - coll: The collection to limit
   
   Returns:
   - If limit is 1: The first item only
   - Otherwise: A sequence of at most 'limit' items
   
   Example:
   (limit-clause 1 [1 2 3])   ;=> 1
   (limit-clause 2 [1 2 3])   ;=> (1 2)
   (limit-clause nil [1 2 3]) ;=> [1 2 3]"
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

(defn query
  "Performs a query on the table with support for filtering, ordering, and 
   pagination.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
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
          :order-by [:date :asc]})"
  [table-name {:keys [limit offset order-by where]}]
  (let [result
        (cond->> (get-all table-name)
          where (filter where)
          order-by (sort-by (first order-by)
                            (case (second order-by)
                              :asc compare
                              :desc #(compare %2 %1)))
          offset (drop offset)
          limit (limit-clause limit))]
    result))

(defn- build-params [acc k v kvs-opts]
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

(defn get-by-key
  "Takes a variable amount of keyval args and the entries that match the 
   keyvals given.
   Accepts the same options as  `query`"
  ([table-name k v]
   (get-by-key table-name k v nil))
  ([table-name k v & kvs-opts]
   (query
    table-name
    (build-params {} k v kvs-opts))))

(defn insert
  "Inserts a new entry into the table with automatic ID generation and 
   timestamps.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
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
           :name \"Jane Doe\"})"
  [table-name data]
  (let [id-kw (id-keyword table-name)
        id (if-let [id (id-kw data)]
             id
             (get-next-id table-name))
        data-with-id (maybe-add-timestamps
                      table-name
                      (assoc data id-kw id))
        entry-file (p/as-file table-name (str id))]
    (spit entry-file (pr-str data-with-id))
    data-with-id))

(defn- update-data [table-name original-data new-data]
  (let [updated
        (if (test/function? new-data)
          (new-data original-data)
          (merge original-data new-data))

        with-updated-timestamps
        (maybe-add-timestamps table-name updated)]

    with-updated-timestamps))

(defn patch
  "Updates an existing entry by merging new data or applying a transformation 
   function.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
   - id: The unique identifier of the entry to update
   - data-or-fn: Either:
                - A map to merge with existing data
                - A function that takes the existing data and returns updated 
                data
   
   Returns:
   - The updated entry data if successful
   - false if the entry doesn't exist
   
   Examples:
   (patch :users 1 {:status :inactive})
   
   (patch :users 1 
         (fn [user] 
           (update user :login-count inc)))"
  [table-name id data-or-fn]
  (let [entry-file (p/as-file table-name (str id))]
    (if (.exists entry-file)
      (let [existing-data (read-string* (slurp entry-file))
            updated-data (update-data table-name existing-data data-or-fn)]
        (spit entry-file (pr-str updated-data))
        updated-data)
      false)))

(defn delete
  "Removes an entry from the table.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
   - id: The unique identifier of the entry to delete
   
   Returns:
   - true if the entry was successfully deleted
   - false if the entry didn't exist
   
   Example:
   (delete :users 1)
   (delete [:orgs :departments] 42)"
  [table-name id]
  (let [entry-file (p/as-file table-name id)]
    (if (.exists entry-file)
      (do
        (.delete entry-file)
        true)
      false)))

(defn reset-db!
  "Completely resets the database by removing all tables and their data.
   Use with caution as this operation cannot be undone.
   
   Returns:
   - nil
   
   Example:
   (reset-db!)"
  []
  (fs/delete-dir p/*db-root*))

(defn drop-table!
  "Removes a table and all its data from the database.
   Use with caution as this operation cannot be undone.
   
   Parameters:
   - table-name: The name of the table to drop
   
   Returns:
   - nil
   
   Example:
   (drop-table! :users)
   (drop-table! [:organizations :departments])"
  [table-name]
  (-> table-name get-config-path fs/delete-dir)
  (let [file (p/as-file table-name)]
    (when (.exists file)
      (-> (fs/delete-dir file)))))

(defn get-count
  "Returns the total number of entries in a table.
   
   Parameters:
   - table-name: The name of the table (string, keyword, or vector for nested 
   tables)
   
   Returns:
   - The number of entries in the table (integer)
   
   Example:
   (get-count :users)
   (get-count [:organizations :departments])"
  [table-name]
  (let [table-path (p/as-file table-name)]
    (->> (fs/list-dir table-path)
         (filter (fn [x]
                   (.isFile x)))
         (count))))
