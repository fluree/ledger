(ns fluree.db.peer.query-peer-tests
  (:require
   [clojure.test :as t :refer [deftest testing is]]
   [environ.core :as environ]
   [fluree.db.server :as server]
   [fluree.db.server-settings :as settings]
   [fluree.db.test-helpers :as test-helpers]
   [fluree.db.util.json :as json]
   [org.httpkit.client :as http]
   [fluree.db.test-helpers :as test]
   [robert.bruce :refer [try-try-again]]))

(defn wait-until-ready
  [api-port ledger]
  (try-try-again
    {:sleep 100, :tries 10}
    (fn []
      (let [resp (some-> (http/post
                           (str "http://localhost:" api-port "/fdb/" ledger
                                "/ledger-stats"))
                         deref
                         (update :body json/parse))]
       (if (= "ready" (some-> resp :body :data :status))
         resp
         (throw (Exception. (str ledger " not ready"))))))))

(deftest query-peer-tests
  (let [ledger-port (test-helpers/get-free-port)
        query-port  (test-helpers/get-free-port)
        _           (println "ledger-port:" ledger-port "query-port:" query-port)
        ledger-peer (test-helpers/start-server {:fdb-api-port ledger-port
                                                :fdb-mode "ledger"
                                                :fdb-group-servers "ledger-server@localhost:11001"
                                                :fdb-group-this-server "ledger-server"
                                                :fdb-storage-type "memory"
                                                :fdb-consensus-type "in-memory"})
        query-peer  (test-helpers/start-server {:fdb-api-port query-port
                                                :fdb-mode "query"
                                                :fdb-query-peer-servers (str "localhost:" ledger-port)
                                                :fdb-group-servers "query-server@localhost:11002"
                                                :fdb-group-this-server "query-server"
                                                :fdb-storage-type "memory"
                                                :fdb-consensus-type "in-memory"})]
    (testing "can create a ledger"
      (let [new-ledger1       @(http/post (str "http://localhost:" query-port "/fdb/new-ledger")
                                          {:headers {"content-type" "application/json"}
                                           :body    (json/stringify {:ledger/id "test/test1"})})
            new-ledger2       @(http/post (str "http://localhost:" ledger-port "/fdb/new-ledger")
                                          {:headers {"content-type" "application/json"}
                                           :body    (json/stringify {:ledger/id "test/test2"})})
            ledger-list-resp1 @(http/post (str "http://localhost:" query-port "/fdb/ledgers"))
            ledger-list-resp2 @(http/post (str "http://localhost:" ledger-port "/fdb/ledgers"))]
        (is (= 200 (:status new-ledger1)))
        (is (= 200 (:status new-ledger2)))
        (is (= [["test" "test1"]
                ["test" "test2"]]
               (-> ledger-list-resp1 :body json/parse)
               (-> ledger-list-resp2 :body json/parse)))
        (is (= "ready" (-> (wait-until-ready query-port "test/test1")
                           :body :data :status)))
        (is (= "ready" (-> (wait-until-ready ledger-port "test/test2")
                           :body :data :status)))))

    (testing "can transact to a ledger"
      (let [transact-resp1 @(http/post (str "http://localhost:" query-port "/fdb/test/test1/transact")
                                       {:headers {"content-type" "application/json"}
                                        :body    (json/stringify [{:_id "_user" :_user/username "query"}])})
            transact-resp2 @(http/post (str "http://localhost:" ledger-port "/fdb/test/test1/transact")
                                       {:headers {"content-type" "application/json"}
                                        :body    (json/stringify [{:_id "_user" :_user/username "ledger"}])})]
        (is (= 200 (:status transact-resp1)))
        (is (= 200 (:status transact-resp2)))))

    (testing "can query a ledger"
      (let [query-query-resp  @(http/post (str "http://localhost:" query-port "/fdb/test/test1/query")
                                          {:headers {"content-type" "application/json"}
                                           :body    (json/stringify {:select ["*"] :from "_user"})})
            ledger-query-resp @(http/post (str "http://localhost:" ledger-port "/fdb/test/test1/query")
                                          {:headers {"content-type" "application/json"}
                                           :body    (json/stringify {:select ["*"] :from "_user"})})]
        (is (= [{:_user/username "ledger"}
                {:_user/username "query"}]
               (->> ledger-query-resp
                    :body
                    json/parse
                    (map #(dissoc % :_id)))
               (->> query-query-resp
                    :body
                    json/parse
                    (map #(dissoc % :_id)))))))

    (test-helpers/stop-server query-peer)
    (test-helpers/stop-server ledger-peer)))
