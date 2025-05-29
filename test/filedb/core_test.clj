(ns filedb.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [filedb.core :as core :refer [->FileDB]]
   [filedb.protocols :as p :refer [default-keywords]]
   [me.raynes.fs :as fs]))

(def test-db-dir "test-fsdb")

(def test-db
  "Test-specific database instance"
  (->FileDB test-db-dir default-keywords))

(defn dispose []
  (fs/delete-dir test-db-dir))

(use-fixtures :each
  (fn [f]
    (f)
    (dispose)))

(deftest test-db-root-location
  (testing "dynamic db-root allows changing database location"
    (p/insert! test-db :test {:name "test-entry"})
    (let [result (p/get-by-key test-db :test :name "test-entry")]
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
        (is (= :test1 (:name (p/insert! test-db table-name row1)))))

      (testing "insert with no primary key"
        (is (= :test2 (:name (p/insert! test-db table-name row2))))))

    (testing "count operations"
      (is (= 2 (p/get-count test-db table-name))))

    (testing "get operations"
      (testing "get-by-id"
        (is (= :test1 (:name (p/get-by-id test-db table-name (:_id row1)))))
        (is (nil? (p/get-by-id test-db table-name "non-existing-id"))))

      (testing "get-all"
        (is (= [1 "custom-id"] (mapv :_id (p/get-all test-db table-name)))))

      (testing "get-by-key"
        (is (= clojure.lang.LazySeq (type (p/get-by-key test-db table-name :name :test1))))
        (is (= :test1 (:name (p/get-by-key test-db table-name :name (:name row1) {:limit 1}))))))

    (testing "patch operations"
      (testing "patch with existing key"
        (is (= :new-name (:name (p/update! test-db table-name (:_id row1) {:name :new-name})))))

      (testing "patch does partial update"
        (is (= (:_id row1) (:_id (p/update! test-db table-name (:_id row1) {:new-key :new-value})))))

      (testing "patch with new key"
        (is (= :new-value (:new-key (p/update! test-db table-name (:_id row1) {:new-key :new-value})))))

      (testing "patch with function"
        (is (= :test1 (:name (p/update! test-db table-name (:_id row1) #(assoc % :name :test1)))))))

    (testing "delete operations"
      (is (true? (p/delete! test-db table-name (:_id row1))))
      (is (false? (p/delete! test-db table-name (:_id row1)))))))

(deftest test-nested-tables
  (testing "nested table operations"
    (p/insert! test-db :a {:name :a}) ; for further testing

    (testing "insert nested table"
      (is (= :nested (:name (p/insert! test-db [:a :b] {:name :nested})))))

    (testing "get operations on nested tables"
      (is (= :nested (-> (p/get-all test-db [:a :b]) first :name)))
      (is (= :nested (-> (p/get-by-key test-db [:a :b] :name :nested) first :name)))
      (is (= 1 (p/get-count test-db :a)))
      (is (= [:a] (->> (p/get-all test-db [:a]) (mapv :name)))))

    (testing "update nested table"
      (p/update! test-db [:a :b] 1 {:name :updated})
      (is (= :updated (:name (p/get-by-id test-db [:a :b] 1)))))

    (testing "delete operations on nested tables"
      (p/insert! test-db [:a :b] {:_id :new :name :new})
      (p/delete! test-db [:a :b] :new)
      (is (nil? (p/get-by-id test-db [:a :b] :new)))
      (is (= 1 (p/get-count test-db [:a :b]))))

    (testing "dropping nested tables"
      (p/insert! test-db [:a :c] {:_id :new :name :new})
      (p/delete-coll! test-db [:a :c])
      (is (empty? (p/get-all test-db [:a :c])))
      (is (= 1 (p/get-count test-db :a)))

      (testing "dropping parent drops nested tables"
        (p/delete-coll! test-db :a)
        (is (nil? (seq (p/get-all test-db [:a :b]))))))))

(deftest test-decentralized-config
  (testing "decentralized config files in __fsdb__"

    (dotimes [n 2]
      (p/insert! test-db :test-table {:name (str "entry " n)}))

    (testing "sequential id generation"
      (is (= "entry 0" (:name (p/get-by-id test-db :test-table 1))))
      (is (= "entry 1" (:name (p/get-by-id test-db :test-table 2)))))

    (testing "config cleanup"
      (testing "drop-table! deletes config"
        (p/delete-coll! test-db :test-table)
        (is (false? (.exists  (io/file (:db-root test-db) "__fsdb__/test-table")))))

      (testing "reset-db! deletes all configs"
        (p/reset-db! test-db)
        (is (false? (.exists (io/file (:db-root test-db) "__fsdb__"))))))))

(deftest test-batch-operations
  (testing "get-by-ids returns multiple entries efficiently"
    (let [ids [11 22 33]]
      (doseq [id ids]
        (p/insert! test-db :test-table {:name (str "# " id) :_id id}))

      (is (= (seq ids) (->> (p/get-by-ids test-db :test-table ids) (map :_id))))

      (p/delete-coll! test-db :test-table))))

