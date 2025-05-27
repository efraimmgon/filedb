(ns filedb.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [filedb.core :refer :all]
   [filedb.protocols :refer [*db-root*]]
   [me.raynes.fs :as fs]))

(def test-db-dir "test-fsdb")

(defn dispose []
  (fs/delete-dir test-db-dir)
  (alter-var-root #'*db-root* (constantly "fsdb")))

(use-fixtures :each
  (fn [f]
    (alter-var-root #'*db-root* (constantly test-db-dir))
    (f)
    (dispose)))

(deftest test-db-root-location
  (testing "dynamic db-root allows changing database location"
    (alter-var-root #'*db-root* (constantly "test-fsdb"))
    (insert :test {:name "test-entry"})
    (let [result (get-by-key :test :name "test-entry")]
      ;; Verify entry was created in test db
      (is (= "test-entry" (-> result first :name)))
      ;; Verify file exists in correct location
      (is (true? (.exists (io/file "test-fsdb")))))))

(deftest test-basic-operations
  (let [table-name :fsdb-test
        row1 {:_id "custom-id" :name :test1}
        row2 {:name :test2}]

    (testing "insert operations"
      (testing "insert with primary key"
        (is (= :test1 (:name (insert table-name row1)))))

      (testing "insert with no primary key"
        (is (= :test2 (:name (insert table-name row2))))))

    (testing "count operations"
      (is (= 2 (get-count table-name))))

    (testing "get operations"
      (testing "get-by-id"
        (is (= :test1 (:name (get-by-id table-name (:_id row1)))))
        (is (nil? (get-by-id table-name "non-existing-id"))))

      (testing "get-all"
        (is (= [1 "custom-id"] (mapv :_id (get-all table-name)))))

      (testing "get-by-key"
        (is (= clojure.lang.LazySeq (type (get-by-key table-name :name :test1))))
        (is (= :test1 (:name (get-by-key table-name :name (:name row1) {:limit 1}))))))

    (testing "patch operations"
      (testing "patch with existing key"
        (is (= :new-name (:name (patch table-name (:_id row1) {:name :new-name})))))

      (testing "patch does partial update"
        (is (= (:_id row1) (:_id (patch table-name (:_id row1) {:new-key :new-value})))))

      (testing "patch with new key"
        (is (= :new-value (:new-key (patch table-name (:_id row1) {:new-key :new-value})))))

      (testing "patch with function"
        (is (= :test1 (:name (patch table-name (:_id row1) #(assoc % :name :test1)))))))

    (testing "delete operations"
      (is (true? (delete table-name (:_id row1))))
      (is (false? (delete table-name (:_id row1)))))))

(deftest test-nested-tables
  (testing "nested table operations"
    (insert :a {:name :a}) ; for further testing

    (testing "insert nested table"
      (is (= :nested (:name (insert [:a :b] {:name :nested})))))

    (testing "get operations on nested tables"
      (is (= :nested (-> (get-all [:a :b]) first :name)))
      (is (= :nested (-> (get-by-key [:a :b] :name :nested) first :name)))
      (is (= 1 (get-count :a)))
      (is (= [:a] (->> (get-all [:a]) (mapv :name)))))

    (testing "update nested table"
      (patch [:a :b] 1 {:name :updated})
      (is (= :updated (:name (get-by-id [:a :b] 1)))))

    (testing "delete operations on nested tables"
      (insert [:a :b] {:_id :new :name :new})
      (delete [:a :b] :new)
      (is (nil? (get-by-id [:a :b] :new)))
      (is (= 1 (get-count [:a :b]))))

    (testing "dropping nested tables"
      (insert [:a :c] {:_id :new :name :new})
      (drop-table! [:a :c])
      (is (empty? (get-all [:a :c])))
      (is (= 1 (get-count :a)))

      (testing "dropping parent drops nested tables"
        (drop-table! :a)
        (is (nil? (seq (get-all [:a :b]))))))))

(deftest test-decentralized-config
  (testing "decentralized config files in __fsdb__"
    (alter-var-root #'*db-root* (constantly "test-fsdb"))

    (dotimes [n 2]
      (insert :test-table {:name (str "entry " n)}))

    (testing "sequential id generation"
      (is (= "entry 0" (:name (get-by-id :test-table 1))))
      (is (= "entry 1" (:name (get-by-id :test-table 2)))))

    (testing "config cleanup"
      (testing "drop-table! deletes config"
        (drop-table! :test-table)
        (is (false? (.exists (io/file *db-root* "__fsdb__/test-table")))))

      (testing "reset-db! deletes all configs"
        (reset-db!)
        (is (false? (.exists (io/file *db-root* "__fsdb__"))))))))

(deftest test-batch-operations
  (testing "get-by-ids returns multiple entries efficiently"
    (let [ids [11 22 33]]
      (doseq [id ids]
        (insert :test-table {:name (str "# " id) :_id id}))

      (is (= (seq ids) (->> (get-by-ids :test-table ids) (map :_id))))

      (drop-table! :test-table))))

