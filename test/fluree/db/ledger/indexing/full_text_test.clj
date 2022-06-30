(ns fluree.db.ledger.indexing.full-text-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb])
  (:import (org.apache.commons.io FileUtils)))

(use-fixtures :once (fn [t]
                      (let [data-dir (test/create-temp-dir)]
                        (test/test-system
                          {:fdb-storage-type        "file"
                           :fdb-consensus-type      "raft"
                           :fdb-storage-file-root   (.getPath data-dir)
                           :fdb-group-log-directory (-> data-dir
                                                        (io/file "group")
                                                        .getPath)}
                          t)
                        (Thread/sleep 100)
                        (FileUtils/deleteDirectory data-dir))))


(deftest ^:integration full-text-search-test
  (testing "Full Text Search"
    (let [ledger (test/rand-ledger test/ledger-chat)]
      (testing "with full text predicates and data"
        (test/transact-schema ledger "chat.edn")
        (test/transact-schema ledger "chat-preds.edn")
        (test/transact-data ledger "chat.edn")
        (Thread/sleep 500) ; Allow the full text indexer to incorporate the
                           ; block in a separate thread.
        (testing "query"
          (let [q {:select "?p"
                   :where [["?p" "fullText:person/fullName" "Doe"]]}
                subject @(-> test/system
                             :conn
                             (fdb/db ledger)
                             (fdb/query q))]
            (is (= (count subject)
                   1)
                "returns a single result")))))))
