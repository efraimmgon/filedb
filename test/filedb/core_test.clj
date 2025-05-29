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
  (->FileDB test-db-dir
            (assoc default-keywords :use-qualified-keyword? false)))

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
  #_(def coll-name :fsdb-test)
  #_(def row2 {:name :test2})
  (let [coll-name :fsdb-test
        row1 {:id "custom-id" :name :test1}
        row2 {:name :test2}]

    (testing "insert operations"
      (testing "insert with primary key"
        (is (= :test1 (:name (p/insert! test-db coll-name row1)))))

      (testing "insert with no primary key"
        (is (= :test2 (:name (p/insert! test-db coll-name row2))))))

    (testing "count operations"
      (is (= 2 (p/get-count test-db coll-name))))

    (testing "get operations"
      (testing "get-by-id"
        (is (= :test1 (:name (p/get-by-id test-db coll-name (:id row1)))))
        (is (nil? (p/get-by-id test-db coll-name "non-existing-id"))))

      (testing "get-all"
        (is (= [1 "custom-id"] (mapv :id (p/get-all test-db coll-name)))))

      (testing "get-by-key"
        (is (= clojure.lang.LazySeq (type (p/get-by-key test-db coll-name :name :test1))))
        (is (= :test1 (:name (p/get-by-key test-db coll-name :name (:name row1) {:limit 1}))))))

    (testing "patch operations"
      (testing "patch with existing key"
        (is (= :new-name (:name (p/update! test-db coll-name (:id row1) {:name :new-name})))))

      (testing "patch does partial update"
        (is (= (:id row1) (:id (p/update! test-db coll-name (:id row1) {:new-key :new-value})))))

      (testing "patch with new key"
        (is (= :new-value (:new-key (p/update! test-db coll-name (:id row1) {:new-key :new-value})))))

      (testing "patch with function"
        (is (= :test1 (:name (p/update! test-db coll-name (:id row1) #(assoc % :name :test1)))))))

    (testing "delete operations"
      (is (true? (p/delete! test-db coll-name (:id row1))))
      (is (false? (p/delete! test-db coll-name (:id row1)))))))

(deftest test-nested-colls
  (testing "nested coll operations"
    (p/insert! test-db :a {:name :a}) ; for further testing

    (testing "insert nested coll"
      (is (= :nested (:name (p/insert! test-db [:a 1 :b] {:name :nested})))))

    (testing "get operations on nested colls"
      (is (= :nested (-> (p/get-all test-db [:a 1 :b]) first :name)))
      (is (= :nested (-> (p/get-by-key test-db [:a 1 :b] :name :nested) first :name)))
      (is (= 1 (p/get-count test-db :a)))
      (is (= [:a] (->> (p/get-all test-db [:a]) (mapv :name)))))

    (testing "update nested coll"
      (p/update! test-db [:a 1 :b] 1 {:name :updated})
      (is (= :updated (:name (p/get-by-id test-db [:a 1 :b] 1)))))

    (testing "delete operations on nested colls"
      (p/insert! test-db [:a 1 :b] {:id :new :name :new})
      (p/delete! test-db [:a 1 :b] :new)
      (is (nil? (p/get-by-id test-db [:a 1 :b] :new)))
      (is (= 1 (p/get-count test-db [:a 1 :b]))))

    (testing "dropping nested colls"
      (p/insert! test-db [:a 1 :c] {:id :new :name :new})
      (p/delete-coll! test-db [:a 1 :c])
      (is (empty? (p/get-all test-db [:a 1 :c])))
      (is (= 1 (p/get-count test-db :a)))

      (testing "dropping parent drops nested colls"
        (p/delete-coll! test-db :a)
        (is (nil? (seq (p/get-all test-db [:a 1 :b]))))))))

(deftest test-decentralized-config
  (testing "decentralized config files in __fsdb__"

    (dotimes [n 2]
      (p/insert! test-db :test-coll {:name (str "entry " n)}))

    (testing "sequential id generation"
      (is (= "entry 0" (:name (p/get-by-id test-db :test-coll 1))))
      (is (= "entry 1" (:name (p/get-by-id test-db :test-coll 2)))))

    (testing "config cleanup"
      (testing "drop-coll! deletes config"
        (p/delete-coll! test-db :test-coll)
        (is (false? (.exists  (io/file (:db-root test-db) "__fsdb__/test-coll")))))

      (testing "reset-db! deletes all configs"
        (p/reset-db! test-db)
        (is (false? (.exists (io/file (:db-root test-db) "__fsdb__"))))))))

(deftest test-batch-operations
  (testing "get-by-ids returns multiple entries efficiently"
    (let [ids [11 22 33]]
      (doseq [id ids]
        (p/insert! test-db :test-coll {:name (str "# " id) :id id}))

      (is (= (seq ids) (->> (p/get-by-ids test-db :test-coll ids) (map :id))))

      (p/delete-coll! test-db :test-coll))))

