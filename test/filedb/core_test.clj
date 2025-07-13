(ns filedb.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [filedb.core :as core]
   [me.raynes.fs :as fs]))

(def test-db-dir "test-fsdb")

(def test-db
  "Test-specific database instance"
  (core/create-db
   {:db-root test-db-dir
    :keyword-strategy (assoc core/default-keywords :coll-ns-type :simple)}))

(defn dispose []
  (fs/delete-dir test-db-dir))

(use-fixtures :each
  (fn [f]
    (f)
    (dispose)))

(deftest test-db-root-location
  (testing "dynamic db-root allows changing database location"
    (core/insert! test-db :test {:name "test-entry"})
    (let [result (core/get-by-key test-db :test :name "test-entry")]
      ;; Verify entry was created in test db
      (is (= "test-entry" (-> result first :name)))
      ;; Verify file exists in correct location
      (is (true? (.exists (io/file "test-fsdb")))))))

(deftest test-basic-operations
  (let [coll-name :fsdb-test
        row1 {:id "custom-id" :name :test1}
        row2 {:name :test2}]

    (testing "insert operations"
      (testing "insert with primary key"
        (is (= :test1 (:name (core/insert! test-db coll-name row1)))))

      (testing "insert with no primary key"
        (is (= :test2 (:name (core/insert! test-db coll-name row2))))))

    (testing "count operations"
      (is (= 2 (core/get-count test-db coll-name))))

    (testing "get operations"
      (testing "get-by-id"
        (is (= :test1 (:name (core/get-by-id test-db coll-name (:id row1)))))
        (is (nil? (core/get-by-id test-db coll-name "non-existing-id"))))

      (testing "get-all"
        (is (= [1 "custom-id"] (mapv :id (core/get-all test-db coll-name)))))

      (testing "get-by-key"
        (is (= clojure.lang.LazySeq (type (core/get-by-key test-db coll-name :name :test1))))
        (is (= :test1 (:name (core/get-by-key test-db coll-name :name (:name row1) {:limit 1}))))))

    (testing "update operations"
      (testing "update with existing key"
        (is (= :new-name (:name (core/update! test-db coll-name (:id row1) {:name :new-name})))))

      (testing "update does partial update"
        (is (= (:id row1) (:id (core/update! test-db coll-name (:id row1) {:new-key :new-value})))))

      (testing "update with new key"
        (is (= :new-value (:new-key (core/update! test-db coll-name (:id row1) {:new-key :new-value})))))

      (testing "update with function"
        (is (= :test1 (:name (core/update! test-db coll-name (:id row1) #(assoc % :name :test1)))))))

    (testing "delete operations"
      (is (true? (core/delete! test-db coll-name (:id row1))))
      (is (false? (core/delete! test-db coll-name (:id row1)))))))

(deftest test-nested-colls
  (testing "nested coll operations"
    (core/insert! test-db :a {:name :a}) ; for further testing

    (testing "insert nested coll"
      (is (= :nested (:name (core/insert! test-db [:a 1 :b] {:name :nested}))))

      (testing "nested directory structure creation"
        (let [nested-dir (io/file test-db-dir "a" "1" "b")]
          (is (true? (.exists nested-dir)))
          (is (true? (.isDirectory nested-dir))))))

    (testing "get operations on nested colls"
      (is (= :nested (-> (core/get-all test-db [:a 1 :b]) first :name)))
      (is (= :nested (-> (core/get-by-key test-db [:a 1 :b] :name :nested) first :name)))
      (is (= 1 (core/get-count test-db :a)))
      (is (= [:a] (->> (core/get-all test-db [:a]) (mapv :name)))))

    (testing "update nested coll"
      (core/update! test-db [:a 1 :b] 1 {:name :updated})
      (is (= :updated (:name (core/get-by-id test-db [:a 1 :b] 1)))))

    (testing "delete operations on nested colls"
      (core/insert! test-db [:a 1 :b] {:id :new :name :new})
      (core/delete! test-db [:a 1 :b] :new)
      (is (nil? (core/get-by-id test-db [:a 1 :b] :new)))
      (is (= 1 (core/get-count test-db [:a 1 :b]))))

    (testing "deleting nested colls"
      (core/insert! test-db [:a 1 :c] {:id :new :name :new})
      (core/delete-coll! test-db [:a 1 :c])
      (is (empty? (core/get-all test-db [:a 1 :c])))
      (is (= 1 (core/get-count test-db :a)))

      (testing "deleting parent deletes nested colls"
        (core/delete-coll! test-db :a)
        (is (nil? (seq (core/get-all test-db [:a 1 :b]))))))))

(deftest test-decentralized-config
  (testing "decentralized config files in __fsdb__"

    (dotimes [n 2]
      (core/insert! test-db :test-coll {:name (str "entry " n)}))

    (testing "sequential id generation"
      (is (= "entry 0" (:name (core/get-by-id test-db :test-coll 1))))
      (is (= "entry 1" (:name (core/get-by-id test-db :test-coll 2)))))

    (testing "config cleanup"
      (testing "drop-coll! deletes config"
        (core/delete-coll! test-db :test-coll)
        (is (false? (.exists  (io/file (:db-root test-db) "__fsdb__/test-coll")))))

      (testing "reset-db! deletes all configs"
        (core/reset-db! test-db)
        (is (false? (.exists (io/file (:db-root test-db) "__fsdb__"))))))))

(deftest test-batch-operations
  (testing "get-by-ids returns multiple entries efficiently"
    (let [ids [11 22 33]]
      (doseq [id ids]
        (core/insert! test-db :test-coll {:name (str "# " id) :id id}))

      (is (= (seq ids) (->> (core/get-by-ids test-db :test-coll ids) (map :id))))

      (core/delete-coll! test-db :test-coll))))

(deftest test-fully-qualified-keywords
  (let [test-db-dir (str test-db-dir "-qualified")
        qualified-db (core/create-db {:db-root test-db-dir})
        coll-name :users
        row #:users{:id "user-1" :name "John" :role "admin"}]

    (testing "insert with qualified keywords"
      (let [inserted (core/insert! qualified-db coll-name row)]
        (is (= "John" (:users/name inserted)))))

    (testing "get operations preserve qualified keywords"
      (let [fetched (core/get-by-id qualified-db coll-name "user-1")]
        (is (= "John" (:users/name fetched)))))

    (testing "update operations maintain qualified keywords"
      (let [updated (core/update! qualified-db coll-name "user-1" {:users/name "Jane"})]
        (is (= "Jane" (:users/name updated)))))

    (testing "get-by-key works with qualified keywords"
      (let [results (core/get-by-key qualified-db coll-name :users/name "Jane")]
        (is (= "Jane" (:users/name (first results))))))

    (testing "nested collections with qualified keywords"
      (let [nested-coll [:users "user-1" :posts]
            post #:posts{:id "post-1"
                         :title "First Post"
                         :content "Hello World"}]

        (testing "insert nested with qualified keywords"
          (let [inserted (core/insert! qualified-db nested-coll post)]
            (is (= "First Post" (:posts/title inserted)))))

        (testing "get nested preserves qualified keywords"
          (let [fetched (core/get-by-id qualified-db nested-coll "post-1")]
            (is (= "First Post" (:posts/title fetched)))))

        (testing "update nested maintains qualified keywords"
          (let [updated (core/update! qualified-db nested-coll "post-1"
                                      {:posts/title "Updated Post"})]
            (is (= "Updated Post" (:posts/title updated)))))

        (testing "get-by-key works with nested qualified keywords"
          (let [results (core/get-by-key qualified-db nested-coll
                                         :posts/title "Updated Post")]
            (is (= "Updated Post" (:posts/title (first results))))))

        (testing "delete nested collection"
          (core/delete-coll! qualified-db nested-coll)
          (is (empty? (core/get-all qualified-db nested-coll))))))

    (fs/delete-dir test-db-dir)))

(deftest test-fully-qualified-keywords
  (let [test-db-dir (str test-db-dir "-qualified")
        qualified-db (core/create-db
                      {:db-root test-db-dir
                       :keyword-strategy (assoc core/default-keywords
                                                :coll-ns-type :full)})
        coll-name :users
        row #:users{:id "user-1" :name "John" :role "admin"}]

    (testing "insert with qualified keywords"
      (let [inserted (core/insert! qualified-db coll-name row)]
        (is (= "John" (:users/name inserted)))))

    (testing "get operations preserve qualified keywords"
      (let [fetched (core/get-by-id qualified-db coll-name "user-1")]
        (is (= "John" (:users/name fetched)))))

    (testing "update operations maintain qualified keywords"
      (let [updated (core/update! qualified-db coll-name "user-1" {:users/name "Jane"})]
        (is (= "Jane" (:users/name updated)))))

    (testing "get-by-key works with qualified keywords"
      (let [results (core/get-by-key qualified-db coll-name :users/name "Jane")]
        (is (= "Jane" (:users/name (first results))))))

    (testing "nested collections with qualified keywords"
      (let [nested-coll [:users "user-1" :posts]
            post #:users.posts{:id "post-1"
                               :title "First Post"
                               :content "Hello World"}]

        (testing "insert nested with qualified keywords"
          (let [inserted (core/insert! qualified-db nested-coll post)]
            (is (= "First Post" (:users.posts/title inserted)))))

        (testing "get nested preserves qualified keywords"
          (let [fetched (core/get-by-id qualified-db nested-coll "post-1")]
            (is (= "First Post" (:users.posts/title fetched)))))

        (testing "update nested maintains qualified keywords"
          (let [updated (core/update! qualified-db nested-coll "post-1"
                                      {:users.posts/title "Updated Post"})]
            (is (= "Updated Post" (:users.posts/title updated)))))

        (testing "get-by-key works with nested qualified keywords"
          (let [results (core/get-by-key qualified-db nested-coll
                                         :users.posts/title "Updated Post")]
            (is (= "Updated Post" (:users.posts/title (first results))))))

        (testing "delete nested collection"
          (core/delete-coll! qualified-db nested-coll)
          (is (empty? (core/get-all qualified-db nested-coll))))))

    (fs/delete-dir test-db-dir)))

(deftest test-uuid-primary-key
  (let [test-db-dir (str test-db-dir "-uuid")
        uuid-db (core/create-db {:db-root test-db-dir
                                 :keyword-strategy (assoc core/default-keywords
                                                          :coll-ns-type :simple)
                                 :primary-key-type :uuid})
        coll-name :users
        test-data {:name "John" :role "admin"}]

    (testing "insert generates valid UUIDs"
      (let [inserted (core/insert! uuid-db coll-name test-data)
            id (:id inserted)]
        (is (string? id))
        (is (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" id))
        (is (= "John" (:name inserted)))))

    (testing "get operations with UUID"
      (let [inserted (core/insert! uuid-db coll-name test-data)
            id (:id inserted)]
        (testing "get-by-id works with UUID"
          (let [fetched (core/get-by-id uuid-db coll-name id)]
            (is (= id (:id fetched)))
            (is (= "John" (:name fetched)))))

        (testing "get-by-ids works with UUIDs"
          (let [second-entry (core/insert! uuid-db coll-name {:name "Jane"})
                ids [id (:id second-entry)]
                fetched (core/get-by-ids uuid-db coll-name ids)]
            (is (= 2 (count fetched)))
            (is (= #{"John" "Jane"} (set (map :name fetched))))))))

    (testing "update operations with UUID"
      (let [inserted (core/insert! uuid-db coll-name test-data)
            id (:id inserted)
            updated (core/update! uuid-db coll-name id {:role "superadmin"})]
        (is (= "superadmin" (:role updated)))
        (is (= id (:id updated)))))

    (testing "delete operations with UUID"
      (let [inserted (core/insert! uuid-db coll-name test-data)
            id (:id inserted)]
        (is (true? (core/delete! uuid-db coll-name id)))
        (is (nil? (core/get-by-id uuid-db coll-name id)))))

    (fs/delete-dir test-db-dir)))
