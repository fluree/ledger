(ns fluree.db.ledger.docs.smart-functions.in-transactions
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]))

(use-fixtures :once test/test-system)

(deftest test-str-?pO
  (testing "Test the str and ?pO functions in a transaction"
    (let [long-desc-txn [{:_id ["person/handle" "jdoe"],
                          :fullName "#(str (?pO) \", Sr.\")"}]
          res (-> (basic/get-conn)
                  (fdb/transact-async test/ledger-chat long-desc-txn)
                  async/<!!)
          flakes (:flakes res)
          pos-flakes (filter #(< 0 (first %)) flakes)]

      (is (= 200 (:status res)))

      (is (= 0 (-> res :tempids count)))

      (is (= 8 (-> res :flakes count)))

      (= #{"Jane Doe" "Jane Doe, Sr."}
         (->> pos-flakes
              (map #(nth % 2))
              set)))))


(deftest test-max
  (testing "Test transactions using max smart function in a transaction"
    (let [count               20
          tx                  (mapv (fn [n]
                                      {:_id      "person"
                                       :fullName (str "#(max " n " "
                                                      (+ 1 n) " " (+ 2 n) ")")})
                                    (range 1 (inc count)))
          tx-result           (-> (basic/get-conn)
                                  (fdb/transact-async test/ledger-chat tx)
                                  async/<!!)
          full-names          (->> tx-result
                                   :flakes
                                   (filter #(= 1002 (second %)))
                                   (map #(nth % 2))
                                   set)
          expected-full-names (->> (range 3 (+ 3 count))
                                   (map str)
                                   set)]

      (is (= full-names expected-full-names)))))

(deftest in-transactions-test
  (test-str-?pO)
  (test-max))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (in-transactions-test))
