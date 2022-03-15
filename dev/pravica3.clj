(ns pravica3
  (:require [fluree.db.util.json :as json]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [fluree.crypto :as crypto]
            [fluree.db.query.schema :as schema]
            [fluree.db.flake :as flake]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]))



(def conn (:conn user/system))
(def ledger "test-chat/v4")

(def sample-user {:auth        "TfHr52sbmL4QPMz4uAKu3rMwdtt9w31bRS6"
                  :private-key "ec08ad981be6def6201aceb5347e59e9b54c8636ca9a4ea09000fbd40cf24069"})

(def db (fdb/db conn ledger))
(def perm-db (fdb/db conn ledger {:auth ["_auth/id" (:auth sample-user)]}))

(comment
  @(fdb/ledger-list conn)

  ;; analytical query same as original, 50050 messages exist in group
  ;; query times - limit: 10 / 100 / 1000
  ;; beta-18: with time filter:   180 / 195 / 280
  ;; newest: with time filter:   310 / 360 / 840
  ;; beta-18: without time filter: 20 / 35 / 120
  ;; newest: without time filter: 55 / 105 / 600
  (time
    (def res
      @(fdb/query
         db
         {:select {"?var" ["*"]}
          :where  [["?var", "message/group", 351843720888321]
                   #_["?var", "message/updatedAt", "#(> ?time 1642340102)"]
                   ],
          :opts   {:limit 10}})))


  ;; analytical query permissioned
  ;; query times - limit: 10 / 100 / 1000
  ;; beta-18: with time filter: 145,000 / 155,000 / 155,000 <- no real difference, same work done regardless of limit
  ;; newest: with time filter: / / /
  ;; beta-18: without time filter: 50,000 / 50,000 / 60,000
  ;; newest: without time filter: / / /
  (time
    (def res
      @(fdb/query
         perm-db
         {:select {"?var" ["*"]}
          :where  [["?var", "message/group", 351843720888321]
                   #_["?var", "message/updatedAt", "#(> ?time 1642340102)"]
                   ],
          :opts   {:limit 10}})))


  ;; basic query
  ;; query times - limit: 10 / 100 / 1000
  ;; beta-18: 90 / 110 / 390
  ;; newest:  230 / 295 / 880
  (time
    (def res
      @(fdb/query
         db
         {:select {"*" [{"_compact" true}]}
          :where  "message/group = 351843720888321 AND message/updatedAt > 1642340102000",
          :opts   {:limit 10}})))


  ;; basic query - permissioned
  ;; query times - limit: 10 / 100 / 1000
  ;; beta-18: 145,000 / 145,000 / 155,000 <- no real difference, same work done regardless of limit
  ;; newest:   /  /
  (time
    (def res
      @(fdb/query
         perm-db
         {:select {"*" [{"_compact" true}]}
          :where  "message/group = 351843720888321 AND message/updatedAt > 1642340102000",
          :opts   {:limit 10}})))


  )