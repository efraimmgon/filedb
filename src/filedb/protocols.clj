(ns filedb.protocols
  (:require
   [clojure.java.io :as io]
   [clojure.string]))

(def default-db-root
  "The root directory for the database.
   Can be dynamically rebound to change the database location.
   
   Default value: \"filedb\"
   
   Example:
   (binding [*db-root* \"custom-db\"]
     (insert :users {...}))"
  "filedb")

(defmulti parse-coll-name
  (fn [colln]
    (type colln)))

(defmethod parse-coll-name java.lang.String [colln] colln)

(defmethod parse-coll-name java.lang.Long [colln] (str colln))

(defmethod parse-coll-name clojure.lang.Symbol [colln] (name colln))

(defmethod parse-coll-name clojure.lang.Keyword [colln] (name colln))

(defmethod parse-coll-name clojure.lang.PersistentVector [colln]
  (->> (for [[i name_] (map-indexed vector colln)
             :when (even? i)]
         (parse-coll-name name_))
       vec))

;; Examples for ids:
;; :user => :user/id
;; [:user 123 :profile] => :user.profile/id

(defn mk-keyword
  "Creates a keyword, optionally qualifying it.
   If `use-qualified-keyword?` is true, the keyword is qualified with a 
   namespace generated from `colln` using `parse-nested-coll`. Otherwise, `k` 
   is returned directly.
   `colln` is either a scalar of the collection name or a odd path-like vector 
   of scalars, e.g., `[:users]` or `[:users 1 :profiles]`.
   `k` is the keyword name, e.g., `:id`."
  [colln k coll-ns-type]
  (case coll-ns-type
    :simple k
    :partial (if-not (vector? colln)
               (keyword (parse-coll-name colln) (name k))
               (keyword (-> colln last parse-coll-name) (name k)))
    :full (if-not (vector? colln)
            (keyword (parse-coll-name colln) (name k))
            (keyword (->> colln parse-coll-name (clojure.string/join "."))
                     (name k)))))

(defprotocol  KeywordStrategy
  (id-keyword [this coll])
  (created-at-keyword [this coll])
  (updated-at-keyword [this coll]))

(defrecord KeywordStrat [id created-at updated-at coll-ns-type]
  KeywordStrategy
  (id-keyword [_ coll] (mk-keyword coll id coll-ns-type))
  (created-at-keyword [_ coll] (mk-keyword coll created-at coll-ns-type))
  (updated-at-keyword [_ coll] (mk-keyword coll updated-at coll-ns-type)))

(def default-keywords
  "Default implementation of KeywordStrategy.
   Uses `:id`, `:created-at`, and `:updated-at` as base keywords
   and enables qualified keywords (e.g., `:user/id`)."
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
