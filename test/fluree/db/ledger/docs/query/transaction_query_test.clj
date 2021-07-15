(ns fluree.db.ledger.docs.query.transaction-query-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.server :as server]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.string :as str]))

(deftest transaction-query-test
  (testing "transaction query"
    (testing "with an initialized ledger"
      (let [{:keys [conn] :as server} (test/start-server {})]

        ;; FIXME: `new-ledger` operations deliver before the ledger is actually
        ;;        created. The 1 second `sleep` is a workaround
        @(fdb/new-ledger conn test/ledger-chat)
        (Thread/sleep 1000)

        (testing "containing a transaction"
          (let [collection-txn [{:_id  "_collection"
                                 :name "person"}
                                {:_id  "_collection"
                                 :name "chat"}
                                {:_id  "_collection"
                                 :name "comment"}
                                {:_id  "_collection"
                                 :name "artist"}
                                {:_id  "_collection"
                                 :name "movie"}]

                {txid :id, :as collection-resp}
                @(fdb/transact conn test/ledger-chat collection-txn)]
            (is (and (not (instance? Throwable collection-resp))
                     (= 200 (:status collection-resp)))
                "transaction is processed without error")
            (is (= 2 (:block collection-resp))
                "has the expected block")
            (is (string? txid)
                "has a valid txid")

            (testing "for a single transaction id"
              (let [query   {:transaction txid}
                    subject (<!! (fdb/transaction-query-async conn test/ledger-chat query))]

                (is (= 200 (:status subject))
                    "returns without error")

                (is (= 12 (-> subject :result :flakes count))
                    "retrieves 12 flakes")))))

        (server/shutdown server)))))
