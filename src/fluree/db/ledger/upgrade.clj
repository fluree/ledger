(ns fluree.db.ledger.upgrade
  (:require [fluree.db.util.log :as log]
            [fluree.db.api :as fdb]
            [fluree.db.storage.core :as storage]
            [fluree.db.flake :as flake]
            [fluree.db.index :as index]
            [fluree.db.util.async :refer [go-try <? <??]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.constants :as const]
            [fluree.db.query.range :as query-range]
            [fluree.db.time-travel :as time-travel]
            [fluree.raft.log :as raft-log]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import java.io.File
           (java.nio.file CopyOption Files Path StandardCopyOption)))

(set! *warn-on-reflection* true)


(defn- new-children-for-branch [left-flake {:keys [children] :as _branch}]
  (loop [[child & r] children
         i        0
         last-rhs nil
         acc      []]
    (if-not child
      acc
      (let [new-child
            (cond
              ;; first segment, place in update farthest 'left-flake'
              (and (zero? i) (not= left-flake (:first child)))
              (do
                (log/info "   -> Updating index segment: " (:id child)
                          "(left flake)")
                (assoc child :first left-flake))

              ;; need to update child, out of sync
              (and last-rhs (not= last-rhs (:first child)))
              (do
                (log/info "   -> Updating index segment: " (:id child))
                (assoc child :first last-rhs))

              ;; no change
              :else
              child)]
        (recur r (inc i) (:rhs child) (conj acc new-child))))))

(defn v1->v2
  "Modifies index segments where the 'rhs' does not match the next segment's first-flake"
  [conn]
  (go-try
    (let [ledgers    (txproto/ledger-list (:group conn)) ;; two-tuples of [network ledger-id]
          left-flake (flake/->Flake (Long/MAX_VALUE) 0 (Long/MAX_VALUE) 0 true nil)]
      (when (not-empty ledgers)
        (log/info "Migrating data version from v1 to v2")
        ;; check every db
        (doseq [[network ledger-id] ledgers]
          (let [ledger-ident (str network "/" ledger-id)
                idx-points   (-> @(fdb/ledger-info conn ledger-ident)
                                 :indexes
                                 keys
                                 sort)]
            (log/info (str "Updating indexes " idx-points " for ledger: " ledger-ident))
            (doseq [idx-point idx-points]
              (log/info (str " - Updating index " idx-point " for ledger: " ledger-ident))
              (let [db-root (<? (storage/read-db-root conn network ledger-id idx-point))]
                (doseq [idx-type index/types]
                  (let [root-idx-key (-> db-root (get idx-type) :id)
                        branch-data  (<? (storage/read-branch conn root-idx-key))
                        new-children (new-children-for-branch left-flake branch-data)
                        branch-data* (assoc branch-data :children new-children)]
                    (<? (storage/write-branch-data conn root-idx-key branch-data*))))))))
        (log/info "Migration complete."))
      (txproto/set-data-version (:group conn) 2))))

(defn rename-nw-or-db
  "Ensures that a name conforms to [a-z0-9-]. Lowercases names, converts _ to -, removes all other special chars"
  [name]
  (-> name
      str/lower-case
      (str/replace #"_" "-")
      (str/replace #"[^a-z0-9-]" "")))


(defn update-ledger-id-state-atom-networks
  [state-atom old-network old-ledger new-network new-ledger]
  (let [old-value (get-in @state-atom [:networks old-network :ledgers old-ledger])
        ;; dissoc old value
        _         (swap! state-atom update-in [:networks old-network :ledgers] dissoc old-ledger)
        ;; assoc new value
        _         (swap! state-atom assoc-in [:networks new-network :ledgers new-ledger] old-value)
        ;; if old network has no ledgers left, delete
        _         (when (= {:ledgers {}} (get-in @state-atom [:networks old-network]))
                    (swap! state-atom update-in [:networks] dissoc old-network))]
    true))


(defn v2->v3
  "Add _shard collection, ensure db names conform to new standard"
  [conn]
  (go-try
    (let [ledger-list @(fdb/ledger-list conn)
          update-txn  [{:_id  "_predicate"
                        :name "_auth/salt"
                        :doc  "Salt used for auth record, if the auth type requires it."
                        :type "bytes"}
                       {:_id                "_predicate"
                        :name               "_auth/type"
                        :doc                "Tag to identify underlying auth record type, if necessary."
                        :type               "tag"
                        :restrictCollection "_auth"
                        :restrictTag        true}
                       {:_id     "_collection"
                        :name    "_shard"
                        :doc     "Shard settings."
                        :version "1"}
                       {:_id                "_predicate"
                        :name               "_collection/shard"
                        :doc                "The shard that this collection is assigned to. If none assigned, defaults to 'default' shard."
                        :type               "ref"
                        :restrictCollection "_shard"}
                       {:_id    "_predicate"
                        :name   "_shard/name"
                        :doc    "Name of this shard"
                        :type   "string"
                        :unique true}
                       {:_id                "_predicate"
                        :name               "_shard/miners"
                        :doc                "Miners (auth records) assigned to this shard"
                        :type               "ref"
                        :restrictCollection "_auth"
                        :multi              true}
                       {:_id  "_predicate"
                        :name "_shard/mutable"
                        :doc  "Whether this shard is mutable. If not specified, defaults to 'false', meaning the data is immutable."
                        :type "boolean"}
                       {:_id    "_predicate"
                        :name   "_setting/id"
                        :doc    "Unique setting id."
                        :type   "string"
                        :unique true}]]
      (when (not-empty ledger-list)
        (log/info "Migrating data version from v2 to v3")
        (loop [[ledger & r] ledger-list]
          (when ledger
            (let [[network ledger-id] ledger
                  db-ident    (str network "/" ledger-id)
                  db-1        (-> (fdb/db conn db-ident)
                                  <?
                                  (time-travel/as-of-block 1)
                                  <?)
                  setting-res (<? (query-range/collection db-1 "_setting"))
                  setting-id  (:s (first setting-res))
                  setting-txn [{:_id setting-id
                                :id  "root"}]]
              (<? (fdb/transact-async conn db-ident update-txn))
              (<? (fdb/transact-async conn db-ident setting-txn))
              (recur r))))
        (txproto/lowercase-all-names (:group conn))
        (log/info "Migration complete."))
      (txproto/set-data-version (:group conn) 3))))


(defn v3->v4
  "Connect just add _tx/hash, as it needs to be subject _id 99."
  [_conn]
  (go-try
    (throw (ex-info "Cannot update ledger from version 3 to version 4. No forwards
    compatible."
                    {:status 400
                     :error  :db/invalid-request}))))

(defn raft?
  [group]
  (some-> group :raft boolean))

(defn raft-log-directory
  [group]
  (-> group :raft :config :log-directory io/file))

(defn db->ledger
  [l]
  (if (= :append-entry (get l 2))
    (update-in l [3 :entry 0] (fn [entry-type]
                                (case entry-type
                                  :new-db         :new-ledger
                                  :initialized-db :initialized-ledger
                                  entry-type)))
    l))

(defn write-entry
  [f l]
  (let [idx        (first l)
        entry-type (get raft-log/entry-types' idx)]
    (case entry-type
      :current-term (let [term (get l 1)]
                      (raft-log/write-current-term f term))
      :voted-for    (let [[_ term _ voted-for] l]
                      (raft-log/write-voted-for f term voted-for))
      :snapshot     (let [[_ snap-term _ snap-idx] l]
                      (raft-log/write-snapshot f snap-idx snap-term))
      (let [[idx _ _ entry] l]
        (raft-log/write-new-command f idx entry)))))

(defn move-file
  [^File source ^File target]
  (Files/move (.toPath source) (.toPath target)
              (into-array CopyOption
                          [(StandardCopyOption/REPLACE_EXISTING)])))

(defn v4->v5
  [{:keys [group] :as _conn}]
  (go-try
   (when (raft? group)
     (let [log-dir   (raft-log-directory group)
           log-files (->> log-dir
                          file-seq
                          (filter (fn [^File f]
                                    (.isFile f))))]
       (doseq [^File f log-files]
         (let [tmp-file (File/createTempFile (.getName f) ".tmp")]
           (->> f
                raft-log/read-log-file
                (map db->ledger)
                (map (partial write-entry tmp-file))
                dorun)
           (move-file tmp-file f)))))
   (txproto/set-data-version group 5)))

(def upgrade-fns
  [v1->v2 v2->v3 v3->v4 v4->v5])

(defn upgrade
  [conn from-v to-v]
  (let [from-v   (or from-v 1)
        to-v     (or to-v const/data_version)  ; v0-9-5-PREVIEW2 was first version
                                               ; marker we used - default
        fn-range (subvec upgrade-fns (dec from-v) (dec to-v))]
    (log/info "Upgrading ledgers from data version" from-v
              "to data version" to-v)
    (doseq [upgrade-fn fn-range]
      (<?? (upgrade-fn conn)))))
