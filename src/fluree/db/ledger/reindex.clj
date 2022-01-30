(ns fluree.db.ledger.reindex
  (:require [fluree.db.util.log :as log]
            [fluree.db.storage.core :as storage]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.session :as session]
            [fluree.db.constants :as const]
            [fluree.db.query.schema :as schema]
            [clojure.core.async :as async]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.bootstrap :as bootstrap])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)

;; 1) start from block 1
;; 2) index each block


(defn filter-collection
  [cid flakes]
  (let [min (flake/min-subject-id cid)
        max (flake/max-subject-id cid)]
    (filter (fn [^Flake flake]
              (and (>= (.-s flake) min)
                   (<= (.-s flake) max)))
            flakes)))


(defn find-pred-prop
  "Finds the predicate id for the specified property (i.e. '_predicate/type')"
  [flakes pred-prop]
  (let [pred-flakes (filter-collection const/$_predicate flakes)
        prop-flake  (->> pred-flakes
                         (filter #(= pred-prop (.-o ^Flake %)))
                         first)]
    (when prop-flake
      (flake/sid->i (.-s ^Flake prop-flake)))))


(defn pred-type-sid
  "Returns the sid for an predicate type, like 'string', or 'ref'"
  [flakes pred-type]
  (let [tags       (filter-collection const/$_tag flakes)
        tag-prefix "_predicate/type:"
        type-str   (str tag-prefix pred-type)
        flake      (some #(when (= (.-o ^Flake %) type-str) %) tags)]
    (when flake
      (.-s ^Flake flake))))


(defn ref-preds
  "Returns a set of predicate IDs that are refs."
  [flakes]
  ;; need to find [_ type-pred-id ref-pred(of tag or ref) _ _ _]
  (let [type-pred-id   (find-pred-prop flakes "_predicate/type") ;; should be 30
        ref-pred-props #{"_predicate/type:tag" "_predicate/type:ref"}
        ref-sids       (->> flakes
                            (filter-collection const/$_tag) ;; tags only
                            (filter #(ref-pred-props (.-o ^Flake %)))
                            (map #(.-s ^Flake %))
                            (into #{}))
        ref-preds      (->> flakes
                            (filter-collection const/$_predicate) ;; only include predicates
                            (filter #(= type-pred-id (.-p ^Flake %))) ;; filter out just _predicate/type flakes
                            (filter #(ref-sids (.-o ^Flake %))) ;; only those whose value is _predicate/type:tag or ref
                            (map #(flake/sid->i (.-s ^Flake %))) ;; turn into pred-id
                            (into #{}))]
    ref-preds))


(defn idx-preds
  "Returns set of pred ids that are either index or unique from genesis block."
  [flakes]
  (let [find-sids       (->> #{"_predicate/index" "_predicate/unique"}
                             (map #(find-pred-prop flakes %))
                             (into #{}))
        index-pred-sids (->> (filter-collection const/$_predicate flakes)
                             (filter #(find-sids (.-p ^Flake %)))
                             (map #(.-s ^Flake %))
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
                         (filter #(ref-pred? (.-p ^Flake %)))
                         (into #{}))
        post-flakes (->> flakes
                         (filter #(idx-pred? (.-p ^Flake %)))
                         (into opst-flakes))
        size        (flake/size-bytes flakes)
        novelty     (:novelty blank-db)
        novelty*    {:spot (into (:spot novelty) flakes)
                     :psot (into (:psot novelty) flakes)
                     :post (into (:post novelty) post-flakes)
                     :opst (into (:opst novelty) opst-flakes)
                     :tspo (into (:tspo novelty) flakes)
                     :size size}
        t           (apply min (map #(.-t ^Flake %) flakes))]
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
     (let [{:keys [network dbid conn]} db
           block-data (if from-ledger
                        (let [[from-network from-ledger-id] (session/resolve-ledger conn from-ledger)]
                          (<? (storage/read-block conn from-network from-ledger-id 1)))
                        (<? (storage/read-block conn network dbid 1)))]
       (when-not block-data
         (throw (ex-info (str "No genesis block present for db: " network "/" dbid)
                         {:status 500
                          :error  :db/unexpected-error})))
       (log/info (str "  -> Reindex ledger: " network "/" dbid " block: 1 containing " (count (:flakes block-data)) " flakes."))
       (let [flakes            (:flakes block-data)
             db*               (with-genesis db flakes)
             schema            (<? (schema/schema-map db*))
             db**              (assoc db* :schema schema)
             indexed-db        (<? (indexing/refresh db** {:status status :message message
                                                           :ecount ecount}))
             group             (-> indexed-db :conn :group)
             network           (:network indexed-db)
             dbid              (:dbid indexed-db)
             index-point       (get-in indexed-db [:stats :indexed])
             state-atom        (-> conn :group :state-atom)
             submission-server (get-in @state-atom [:_work :networks network])]
         ;; do a baseline index of first block
         (txproto/write-index-point-async group network dbid index-point submission-server {})
         indexed-db)))))


(defn reindex
  ([conn network dbid]
   (reindex conn network dbid {:status "ready"}))
  ([conn network dbid {:keys [status message ecount novelty-max]}]
   (go-try
     (let [sess        (session/session conn (str network "/" dbid))

           blank-db    (:blank-db sess)
           max-novelty (or novelty-max (-> conn :meta :novelty-max)) ;; here we are a little extra aggressive and will go over max
           _           (when-not max-novelty
                         (throw (ex-info "No max novelty set, unable to reindex."
                                         {:status 500
                                          :error  :db/unexpected-error})))
           genesis-db  (<? (write-genesis-block blank-db {:status  status
                                                          :message message
                                                          :ecount  ecount}))]
       (log/info (str "-->> Reindex starting dbid: " dbid ". Max novelty: " max-novelty))
       (loop [block 2
              db    genesis-db]
         (let [block-data (<? (storage/read-block conn network dbid block))]
           (if (nil? block-data)
             (do (log/info (str "-->> Reindex finished dbid: " dbid " block: " (dec block)))
                 (if (> (get-in db [:novelty :size]) 0)
                   (let [indexed-db        (async/<! (indexing/refresh db {:status  status
                                                                           :message message
                                                                           :ecount  ecount}))
                         group             (-> indexed-db :conn :group)
                         network           (:network indexed-db)
                         dbid              (:dbid indexed-db)
                         index-point       (get-in indexed-db [:stats :indexed])
                         state-atom        (-> conn :group :state-atom)
                         submission-server (get-in @state-atom [:_work :networks network])]
                     (<? (txproto/write-index-point-async group network dbid index-point submission-server {}))
                     indexed-db)                            ;; final index if any novelty
                   db))
             (let [{:keys [flakes]} block-data
                   db*          (<? (dbproto/-with db block flakes {:reindex? true}))
                   novelty-size (get-in db* [:novelty :size])]
               (log/info (str "  -> Reindex dbid: " dbid
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
   (doseq [[network dbid] (->> conn
                               txproto/ledgers-info-map
                               (map (juxt :network :ledger)))]
     (log/info "Rebuilding indexes for ledger [" network dbid "]")
     (let [status (<? (reindex conn network dbid))]
       (log/info "Ledger rebuilding complete for ledger [" network dbid "]"
                 status)))))
