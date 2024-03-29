(ns fluree.db.ledger.reindex
  (:require [fluree.db.util.log :as log]
            [fluree.db.storage.core :as storage]
            [fluree.db.ledger.storage :as ledger-storage]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.index :as index]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.session :as session]
            [fluree.db.constants :as const]
            [fluree.db.query.schema :as schema]
            [clojure.core.async :as async]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.bootstrap :as bootstrap]
            [fluree.db.ledger.garbage-collect :as gc]
            [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn filter-collection
  [cid flakes]
  (let [min (flake/min-subject-id cid)
        max (flake/max-subject-id cid)]
    (filter (fn [flake]
              (and (>= (flake/s flake) min)
                   (<= (flake/s flake) max)))
            flakes)))


(defn find-pred-prop
  "Finds the predicate id for the specified property (i.e. '_predicate/type')"
  [flakes pred-prop]
  (let [pred-flakes (filter-collection const/$_predicate flakes)
        prop-flake  (->> pred-flakes
                         (filter #(= pred-prop (flake/o %)))
                         first)]
    (when prop-flake
      (-> prop-flake flake/s flake/sid->i))))


(defn pred-type-sid
  "Returns the sid for an predicate type, like 'string', or 'ref'"
  [flakes pred-type]
  (let [tags       (filter-collection const/$_tag flakes)
        tag-prefix "_predicate/type:"
        type-str   (str tag-prefix pred-type)
        flake      (some #(when (= (flake/o %) type-str) %) tags)]
    (when flake
      (flake/s flake))))


(defn ref-preds
  "Returns a set of predicate IDs that are refs."
  [flakes]
  ;; need to find [_ type-pred-id ref-pred(of tag or ref) _ _ _]
  (let [type-pred-id   (find-pred-prop flakes "_predicate/type") ;; should be 30
        ref-pred-props #{"_predicate/type:tag" "_predicate/type:ref"}
        ref-sids       (->> flakes
                            (filter-collection const/$_tag) ;; tags only
                            (filter #(ref-pred-props (flake/o %)))
                            (map #(flake/s %))
                            (into #{}))
        ref-preds      (->> flakes
                            (filter-collection const/$_predicate) ;; only include predicates
                            (filter #(= type-pred-id (flake/p %))) ;; filter out just _predicate/type flakes
                            (filter #(ref-sids (flake/o %))) ;; only those whose value is _predicate/type:tag or ref
                            (map #(flake/sid->i (flake/s %))) ;; turn into pred-id
                            (into #{}))]
    ref-preds))


(defn idx-preds
  "Returns set of pred ids that are either index or unique from genesis block."
  [flakes]
  (let [find-sids       (->> #{"_predicate/index" "_predicate/unique"}
                             (map #(find-pred-prop flakes %))
                             (into #{}))
        index-pred-sids (->> (filter-collection const/$_predicate flakes)
                             (filter #(find-sids (flake/p %)))
                             (map #(flake/s %))
                             (map flake/sid->i)
                             (into #{}))]
    ;; add in refs
    (into index-pred-sids (ref-preds flakes))))


(defn with-genesis
  "The genesis block can't be processed with the normal -with as
  it is empty. This bypasses all checks and just generates the new index."
  [blank-db flakes]
  (let [ref-pred?   (ref-preds flakes)
        idx-pred?   (idx-preds flakes)
        opst-flakes (->> flakes
                         (filter #(ref-pred? (flake/p %)))
                         (into #{}))
        post-flakes (->> flakes
                         (filter #(idx-pred? (flake/p %)))
                         (into opst-flakes))
        size        (flake/size-bytes flakes)
        novelty     (:novelty blank-db)
        novelty*    {:spot (into (:spot novelty) flakes)
                     :psot (into (:psot novelty) flakes)
                     :post (into (:post novelty) post-flakes)
                     :opst (into (:opst novelty) opst-flakes)
                     :tspo (into (:tspo novelty) flakes)
                     :size size}
        t           (apply min (map #(flake/t %) flakes))]
    (assoc blank-db :block 1
                    :t t
                    :ecount bootstrap/genesis-ecount
                    :novelty novelty*
                    :stats {:flakes (count flakes)
                            :size   size})))


(defn write-genesis-block
  "Writes an initial index with a genesis block.

  If an optional from-ledger is provided (for a forked ledger),
  uses the data from that ledger to generate the initial index."
  ([db] (write-genesis-block db {}))
  ([db {:keys [status message from-ledger ecount]}]
   (go-try
     (let [{:keys [network ledger-id conn]} db
           block-data (if from-ledger
                        (let [[from-network from-ledger-id] (session/resolve-ledger conn from-ledger)]
                          (<? (storage/read-block conn from-network from-ledger-id 1)))
                        (<? (storage/read-block conn network ledger-id 1)))]
       (when-not block-data
         (throw (ex-info (str "No genesis block present for db: " network "/" ledger-id)
                         {:status 500
                          :error  :db/unexpected-error})))
       (log/info (str "  -> Reindex ledger: " network "/" ledger-id " block: 1 containing " (count (:flakes block-data)) " flakes."))
       (let [flakes            (:flakes block-data)
             db*               (with-genesis db flakes)
             schema            (<? (schema/schema-map db*))
             db**              (assoc db* :schema schema)
             indexed-db        (<? (indexing/refresh db** {:status status :message message
                                                           :ecount ecount}))
             group             (-> indexed-db :conn :group)
             network           (:network indexed-db)
             ledger-id         (:ledger-id indexed-db)
             index-point       (get-in indexed-db [:stats :indexed])
             state-atom        (-> conn :group :state-atom)
             submission-server (get-in @state-atom [:_work :networks network])]
         ;; do a baseline index of first block
         (txproto/write-index-point-async group network ledger-id index-point submission-server {})
         indexed-db)))))

(defn stale-index-ids
  [{:keys [conn network ledger-id] :as db} idx]
  (go-try
    (log/debug "Finding stale node for index" idx)
    (let [{:keys [storage-list]} conn

          root        (get db idx)
          always      (constantly true)
          error-ch    (async/chan)
          id-ch       (->> (index/tree-chan conn root always always
                                            1 (map :id) error-ch)
                           (async/into #{}))
          current-ids (async/alt!
                        error-ch
                        ([e] (throw e))

                        id-ch
                        ([ids] ids))
          idx-name    (name idx)
          files       (<? (->> [network ledger-id idx-name]
                               (str/join "/")
                               storage-list))]
      (->> files
           (map :name)
           (remove #{idx-name})
           (map (fn [f]
                  (str/replace (str/join "_" [network ledger-id idx-name f])
                               #"\.fdbd" "")))
           (remove current-ids)))))

(defn remove-stale-files
  [{:keys [conn] :as db} idx]
  (go-try
    (log/info "Removing stale files for index" idx)
    (let [stale-ids (<? (stale-index-ids db idx))]
      (doseq [id stale-ids]
        (log/trace "Deleting stale index node" id)
        (<? (gc/delete-file-raft conn id))))))

(defn reindex
  ([conn network ledger-id]
   (reindex conn network ledger-id {:status "ready"}))
  ([conn network ledger-id {:keys [status message ecount novelty-max]}]
   (go-try
     (let [sess        (session/session conn (str network "/" ledger-id))

           blank-db    (:blank-db sess)
           max-novelty (or novelty-max (-> conn :meta :novelty-max)) ;; here we are a little extra aggressive and will go over max
           _           (when-not max-novelty
                         (throw (ex-info "No max novelty set, unable to reindex."
                                         {:status 500
                                          :error  :db/unexpected-error})))
           genesis-db  (<? (write-genesis-block blank-db {:status  status
                                                          :message message
                                                          :ecount  ecount}))]
       (log/info (str "-->> Reindex starting ledger-id: " ledger-id ". Max novelty: " max-novelty))
       (loop [block 2
              db    genesis-db]
         (let [block-data (<? (storage/read-block conn network ledger-id block))]
           (if (nil? block-data)
             (do (log/info (str "-->> Reindex finished ledger-id: " ledger-id " block: " (dec block)))
                 (let [final-db (if (> (get-in db [:novelty :size]) 0)
                                  (let [indexed-db        (<? (indexing/refresh db {:status  status
                                                                                    :message message
                                                                                    :ecount  ecount}))
                                        group             (-> indexed-db :conn :group)
                                        network           (:network indexed-db)
                                        ledger-id         (:ledger-id indexed-db)
                                        index-point       (get-in indexed-db [:stats :indexed])
                                        state-atom        (-> conn :group :state-atom)
                                        submission-server (get-in @state-atom [:_work :networks network])]
                                    (<? (txproto/write-index-point-async group network ledger-id index-point
                                                                         submission-server {}))
                                    indexed-db) ; final index if any novelty
                                  db)]
                   (log/info "Removing stale index files")
                   (doseq [idx index/types]
                     (<? (remove-stale-files final-db idx)))
                   final-db))
             (let [{:keys [flakes]} block-data
                   db*          (<? (dbproto/-with db block flakes {:reindex? true}))
                   novelty-size (get-in db* [:novelty :size])]
               (log/info (str "  -> Reindex ledger-id: " ledger-id
                              " block: " block
                              " containing " (count flakes)
                              " flakes. Novelty size: " novelty-size "."))
               (if (>= novelty-size max-novelty)
                 (let [db**  (<? (indexing/refresh db*))
                       group (-> db** :conn :group)]
                   (txproto/write-index-point-async group db**)
                   (recur (inc block) db**))
                 (recur (inc block) db*))))))))))

(defn reindex-all
  [conn]
  (go-try
    (doseq [[network ledger-id] (->> conn
                                     txproto/ledgers-info-map
                                     (map (juxt :network :ledger)))]
      (log/info "Rebuilding indexes for ledger [" network ledger-id "]")
      (let [status (<? (reindex conn network ledger-id))]
        (log/info "Ledger rebuilding complete for ledger [" network ledger-id "]"
                  status)))))
