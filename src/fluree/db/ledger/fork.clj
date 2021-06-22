(ns fluree.db.ledger.fork
  (:require [clojure.tools.logging :as log]
            [fluree.db.storage.core :as storage]
            [fluree.db.ledger.reindex :as reindex]
            [fluree.db.session :as session]
            [fluree.db.api :as fdb]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.ledger.indexing :as indexing]
            [clojure.core.async :as async]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.crypto :as crypto]
            [clojure.string :as str]))


;(defn fork-blocks
;  "Copies blocks from fork-dbid to dbid up to block to-block (optional)."
;  [conn network dbid fork-dbid to-block]
;  (log/info "->> Fork start. From:" fork-dbid "to:" dbid "through block: " to-block)
;  (loop [block 1]
;    (if (and to-block (> block to-block))
;      (log/info "->> Fork blocks finished. From:" fork-dbid "to:" dbid "block:" (dec block))
;      (let [old (storage/block conn network fork-dbid block)]
;        (if (nil? old)
;          (log/info "->> Fork blocks finished. From:" fork-dbid "to:" dbid "block:" (dec block))
;          (let [w (storage/write-block conn network dbid old)]
;            (log/info "-> Fork blocks from:" fork-dbid "to:" dbid "block:" block "result:" w)
;            (recur (inc block))))))))


(defn find-closest-index
  "Finds closes index if available for the database being forked."
  [conn network dbid target-block]
  (go-try (let [dbstatus   (txproto/ledger-status (:group conn) network dbid)
                last-index (:index dbstatus)]
            (loop [prev-index last-index]
              (let [db-root (storage/read-db-root conn network dbid prev-index)]
                (cond
                  (nil? db-root) nil                        ;; prev index must have been garbage collected

                  (>= target-block prev-index)
                  prev-index

                  :else
                  (recur (:prevIndex db-root))))))))


;; TODO - can use ledger-status api call for this instead
(defn find-latest-db-block
  "Returns block of the most current known db."
  [conn ledger]
  (go-try
    (let [db (<? (fdb/db conn ledger))]
      (:block db))))


;; TODO - essentially this is the same as reindex/reindex, except blocks read from different db
;; TODO - possibly combine
(defn forked-reindex
  "Reindex of a forked database."
  [conn ledger fork-ledger to-block]
  (go-try
    (let [forked-session   (session/session conn fork-ledger)
          forked-network   (:network forked-session)
          forked-ledger-id (:dbid forked-session)
          session          (session/session conn ledger)
          blank-db         (:blank-db session)
          max-novelty      (-> conn :meta :novelty-max)     ;; here we are a little extra aggressive and will go over max
          _                (when-not max-novelty
                             (throw (ex-info "No max novelty set, unable to reindex."
                                             {:status 500
                                              :error  :db/unexpected-error})))
          db               (<? (reindex/write-genesis-block
                                 blank-db
                                 {:status "forking"
                                  :message "Database is forking."
                                  :from-ledger fork-ledger}))]
      (loop [block 2
             db    db]
        (let [block-data (<? (storage/read-block conn forked-network forked-ledger-id block))]
          (if (or (> block to-block) (nil? block-data))
            (do (log/info (str "-->> Forked db index finished: " ledger " block: " (dec block)))
                (if (> (get-in db [:novelty :size]) 0)
                  (->> (async/<! (indexing/index db {:status "ready"}))
                       (txproto/write-index-point-async (-> db :conn :group))
                       (async/<!))
                  db))
            (let [{:keys [flakes t]} block-data
                  db*          (<? (dbproto/-with db block flakes))
                  novelty-size (get-in db* [:novelty :size])]
              (log/info (str "  -> Reindex db: " ledger " block: " block " containing " (count flakes) " flakes. Novelty size: " novelty-size "."))
              (if (>= novelty-size max-novelty)
                (recur (inc block) (let [indexed-db (async/<! (indexing/index db*))]
                                     (async/<!! (txproto/write-index-point-async (-> indexed-db :conn :group) indexed-db))
                                     indexed-db))
                (recur (inc block) db*)))))))))


(defn copy-db
  [conn ledger copy-ledger to-block command]
  (go-try
    (log/warn "&&& Copy Start!")
    (let [[fork-network fork-ledger-id] (session/resolve-ledger conn copy-ledger)
          [network ledger-id] (session/resolve-ledger conn ledger)]
      (log/warn "Forking: " fork-network fork-ledger-id " To: " network ledger-id)
      (when-not (txproto/ledger-exists? (:group conn) fork-network fork-ledger-id)
        (throw (ex-info (str "Cannot fork ledger: " copy-ledger ", it cannot be found!")
                        {:status 400
                         :error  :db/invalid-action})))
      (when (and to-block (not (pos-int? to-block)))
        (throw (ex-info (str "Block to fork at must be a positive integer. Supplied: " to-block ".")
                        {:status 400
                         :error  :db/invalid-action})))

      (let [latest-block (<? (find-latest-db-block conn copy-ledger))
            to-block*    (if to-block
                           (min to-block latest-block)
                           latest-block)
            _            (loop [block 1]
                           (if (< to-block* block)
                             (<? (txproto/register-genesis-block-async (:group conn) network ledger-id))
                             (let [block-data  (<? (storage/read-block conn fork-network fork-ledger-id block))
                                   write-block (<? (storage/write-block conn network ledger-id block-data))]
                               (recur (inc block)))))

            ;; TODO - use closest index
            ;; If a closest-index we can use, start there, apply additional blocks and write out a new index.
            ;closest-index (<? (find-closest-index conn fork-network fork-ledger-id to-block*))
            ]

        (let [[network dbid] (str/split ledger "/")
              indexed-db (<?? (reindex/reindex conn network dbid))
              txid       (crypto/sha3-256 (:cmd command))
              group      (-> indexed-db :conn :group)
              block      (:block indexed-db)
              _          (log/info " txid network ledger-id block (:fork indexed-db) (get-in indexed-db [:stats :indexed])" txid network ledger-id block (:fork indexed-db) (get-in indexed-db [:stats :indexed]))
              _          (<?? (txproto/initialized-ledger-async group txid network ledger-id block (:fork indexed-db) (get-in indexed-db [:stats :indexed])))
              sess       (session/session conn ledger)
              _          (session/close sess)]
          indexed-db)))))


(defn fork
  "Forks fork-dbid up to optional to-block (else forks all blocks)
  over to dbid.

  Note any existing db at dbid will be overwritten, so some verification should happen
  before this point that dbid does not exist."
  [conn ledger forked-ledger to-block]
  (go-try
    (log/warn "&&& Fork Start!")
    (let [[fork-network fork-ledger-id] (session/resolve-ledger conn forked-ledger)
          [network ledger-id] (session/resolve-ledger conn ledger)]
      (log/warn "Forking: " fork-network fork-ledger-id " To: " network ledger-id)
      (when-not (txproto/ledger-exists? (:group conn) fork-network fork-ledger-id)
        (throw (ex-info (str "Cannot fork ledger: " forked-ledger ", it cannot be found!")
                        {:status 400
                         :error  :db/invalid-action})))
      (when (and to-block (not (pos-int? to-block)))
        (throw (ex-info (str "Block to fork at must be a positive integer. Supplied: " to-block ".")
                        {:status 400
                         :error  :db/invalid-action})))

      (let [latest-block  (<? (find-latest-db-block conn forked-ledger))
            to-block*     (if to-block
                            (min to-block latest-block)
                            latest-block)
            closest-index (find-closest-index conn fork-network fork-ledger-id to-block*)]
        ;; If a closest-index we can use, start there, apply additional blocks and write out a new index.
        (if closest-index
          (let [new-root (-> (storage/read-db-root conn fork-network fork-ledger-id closest-index)
                             (assoc :network network
                                    :dbid ledger-id
                                    :fork forked-ledger
                                    :fork-block to-block*
                                    :conn conn))]
            (if (= closest-index to-block*)
              ;; all done, write root
              (do
                (<? (storage/write-db-root new-root))
                (async/<! (txproto/write-index-point-async (:group conn) new-root)))
              ;; else need to apply latest blocks
              (let [session  (session/session conn ledger)
                    blank-db (:blank-db session)
                    new-db   (storage/reify-db-root conn blank-db new-root)]
                (loop [db    new-db
                       block (inc closest-index)]
                  (let [block-data (<? (storage/read-block conn fork-network fork-ledger-id block))]
                    (if (or (> block to-block*) (nil? block-data))
                      (let [indexed-db (async/<!! (indexing/index db))]
                        (async/<! (txproto/write-index-point-async (:group conn) indexed-db))
                        indexed-db)
                      (recur (<? (dbproto/-with db block (:flakes block-data))) (inc block))))))))
          ;; no closest index, we need to rebuild entire db
          (forked-reindex conn ledger forked-ledger latest-block))))))

