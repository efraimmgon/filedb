(ns filedb.core
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :as test]
   [me.raynes.fs :as fs]))


(defn read-string*
  "Reads an EDN string with support for #inst literals."
  [s]
  (edn/read-string
   {:readers {'inst #(java.time.Instant/parse %)}}
   s))


;;; ----------------------------------------------------------------------------
;;; Utils
;;; ----------------------------------------------------------------------------

(defn mkvec
  "Wraps x in a vector if it's not a vector."
  [x]
  (if (vector? x)
    [x]
    x))


(defn deep-merge
  "Recursively merges maps and vectors."
  [a b]
  (merge-with (fn [x y]
                (cond (map? y) (deep-merge x y)
                      (vector? y) (concat x y)
                      :else y))
              a b))


(def ^:dynamic *db-root*
  "The root directory for the database."
  "fsdb")


(defn ->str
  "Converts a value to a string."
  [x]
  (cond
    (keyword? x) (name x)
    (symbol? x) (name x)
    :else x))


(defn- id-keyword
  "Returns the ID keyword for a table."
  [_table-name]
  :_id)


(defn- created-at-keyword
  "Returns the created-at keyword for a table."
  [_table-name]
  :_created-at)


(defn- updated-at-keyword
  "Returns the updated-at keyword for a table."
  [_table-name]
  :_updated-at)


(defn get-table-path
  "Returns the path to a table. Supports nested tables via vector notation."
  [table-name]
  (let [path-parts (if (vector? table-name)
                     (mapv ->str table-name)
                     [(->str table-name)])
        table-path (apply io/file *db-root* path-parts)]
    (when-not (.exists table-path)
      (.mkdirs table-path))
    table-path))


(defn get-config-path
  "Returns the path to a table configuration directory."
  [table-name]
  (get-table-path
   (into ["__fsdb__"] (mkvec table-name))))


(defn get-next-id
  "Returns the next ID for a table."
  [table-name]
  (let [counter-path (get-table-path
                      (into ["__fsdb__"] (mkvec table-name)))
        counter-file (io/file counter-path "__counter__.edn")]
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
  "Returns the current timestamp."
  []
  (java.time.Instant/now))


(defn maybe-add-created-at
  "Adds the created-at field to the data if it doesn't exist."
  [data k timestamp]
  (if (contains? data k)
    data
    (assoc data k timestamp)))


(defn add-updated-at
  "Adds the updated-at field to the data."
  [data k timestamp]
  (assoc data k timestamp))


(defn maybe-add-timestamps
  "Adds the created-at and updated-at fields to the data."
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
  "Returns the entry with the given ID."
  [table-name id]
  (when id
    (let [entry-file (io/file (get-table-path table-name) (str id))]
      (newline)
      (when (.exists entry-file)
        (read-string* (slurp entry-file))))))


(defn get-by-ids
  "Returns entries with the given IDs. More efficient than querying multiple 
   times."
  [table-name ids]
  (let [table-path (get-table-path table-name)]
    (->> ids
         (map str)
         (map #(io/file table-path %))
         (filter #(.exists %))
         (map #(read-string* (slurp %))))))


(defn get-all
  "Returns all entries in the table."
  [table-name]
  (let [table-path (get-table-path table-name)]
    (->> (.listFiles table-path)
         (filter #(.isFile %))
         (remove #(= (.getName %) "_counter.edn"))
         (map #(read-string* (slurp %))))))


(defn limit-clause
  "Applies the limit clause for a query."
  [limit coll]
  (if (= limit 1)
    (first coll)
    (take limit coll)))


(defn kv=
  "Utility function for where clause in query.
   Returns a function that checks if a key-value pair is equal.
   
   Example;
   (query :user {:where (kv= :email me@example.com)})"
  [k v]
  (fn [ctx]
    (= (get ctx k) v)))


(defn query
  "Returns the entries that match the query."
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
  "Inserts a new entry into the table.
   Primary key is auto-generated, unless the :_id keyword is present in 
   the data."
  [table-name data]
  (let [id-kw (id-keyword table-name)
        id (if-let [id (id-kw data)]
             id
             (get-next-id table-name))
        data-with-id (maybe-add-timestamps
                      table-name
                      (assoc data id-kw id))
        entry-file (io/file (get-table-path table-name) (str id))]
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
  "Patches an existing entry."
  [table-name id data-or-fn]
  (let [entry-file (io/file (get-table-path table-name) (str id))]

    (if (.exists entry-file)

      (let [existing-data (read-string* (slurp entry-file))
            updated-data (update-data table-name existing-data data-or-fn)]
        (spit entry-file (pr-str updated-data))
        updated-data)

      false)))


(defn delete
  "Deletes an entry from the table."
  [table-name id]
  (let [entry-file (io/file (get-table-path table-name) (str id))]
    (if (.exists entry-file)
      (do
        (.delete entry-file)
        true)
      false)))


(defn reset-db!
  "Resets the database by deleting all files in the tables directory."
  []
  (fs/delete-dir *db-root*))


(defn drop-table!
  "Resets the table by deleting all files in the table directory."
  [table-name]
  (-> table-name get-config-path fs/delete-dir)
  (let [file (get-table-path table-name)]
    (when (.exists file)
      (-> (fs/delete-dir file)))))


(defn get-count
  "Returns the number of entries in the table."
  [table-name]
  (let [table-path (get-table-path table-name)]
    (->> (fs/list-dir table-path)
         (filter (fn [x]
                   (and (.isFile x)
                        (not= "_counter.edn"
                              (.getName x)))))
         (count))))
