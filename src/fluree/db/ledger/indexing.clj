(ns fluree.db.ledger.indexing
  (:require [clojure.data.avl :as avl]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.index :as index]
            [fluree.db.storage.core :as storage]
            [fluree.db.session :as session]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [>! <! chan go go-loop]]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto])
  (:import (java.time Instant)))

(set! *warn-on-reflection* true)

(def ^:dynamic *overflow-bytes* 500000)
(defn overflow-leaf?
  [{:keys [flakes]}]
  (> (flake/size-bytes flakes) *overflow-bytes*))

(def ^:dynamic *underflow-bytes* 50000)
(defn underflow-leaf?
  [{:keys [flakes]}]
  (< (flake/size-bytes flakes) *underflow-bytes*))

(def ^:dynamic *overflow-children* 500)
(defn overflow-children?
  [children]
  (> (count children) *overflow-children*))

(defn dirty?
  "Returns `true` if the index for `db` of type `idx` is out of date, or if `db`
  has any out of date index if `idx` is unspecified. Returns `false` otherwise."
  ([db idx]
   (-> db
       :novelty
       (get idx)
       seq
       boolean))
  ([db]
   (->> index/types
        (some (partial dirty? db))
        boolean)))

(defn mark-expanded
  [node]
  (assoc node ::expanded true))

(defn unmark-expanded
  [node]
  (dissoc node ::expanded))

(defn expanded?
  [node]
  (or (index/leaf? node)
      (-> node ::expanded true?)))

(defn resolve-if-novel
  "Resolves data associated with `node` from storage configured in the connection
  `conn` if the `novelty` set contains flakes within the range of node or the
  `remove-preds` sequence is nonempty. Any errors are put on the `error-ch`"
  [conn node t novelty remove-preds error-ch]
  (go
    (let [node-novelty (index/novelty-subrange node t novelty)]
      (if (or (seq node-novelty) (seq remove-preds))
        (try*
         (log/debug "Resolving index node" (select-keys node [:id :network :dbid]))
         (let [{:keys [id] :as resolved-node} (<? (index/resolve conn node))]
           (assoc resolved-node ::old-id id))
         (catch* e
                 (log/error e
                            "Error resolving novel index node:"
                            (select-keys node [:id :network :dbid]))
                 (>! error-ch e)))
        node))))

(defn resolve-children
  "Resolves a branch's children in parallel, and loads data for each child only if
  there are novelty flakes associated with that child. Returns a channel that
  will eventually contain a vector of resolved children."
  [conn branch t novelty remove-preds error-ch]
  (->> branch
       :children
       (map (fn [[_ child]]
              (resolve-if-novel conn child t novelty remove-preds error-ch)))
       (async/map vector)))

(defn filter-predicates
  [preds & flake-sets]
  (if (seq preds)
    (->> flake-sets
         (apply concat)
         (filter (fn [f]
                   (contains? preds (flake/p f)))))
    []))

(defn rebalance-leaf
  "Splits leaf nodes if the combined size of it's flakes is greater than
  `*overflow-bytes*`."
  [{:keys [flakes leftmost? rhs] :as leaf}]
  (if (overflow-leaf? leaf)
    (let [target-size (/ *overflow-bytes* 2)]
      (log/debug "Rebalancing index leaf:"
                 (select-keys leaf [:id :network :dbid]))
      (loop [[f & r] flakes
             cur-size  0
             cur-first f
             leaves    []]
        (if (empty? r)
          (let [subrange  (flake/subrange flakes >= cur-first)
                last-leaf (-> leaf
                              (assoc :flakes subrange
                                     :first cur-first
                                     :rhs rhs)
                              (dissoc :id :leftmost?))]
            (conj leaves last-leaf))
          (let [new-size (-> f flake/size-flake (+ cur-size) long)]
            (if (> new-size target-size)
              (let [subrange (flake/subrange flakes >= cur-first < f)
                    new-leaf (-> leaf
                                 (assoc :flakes subrange
                                        :first cur-first
                                        :rhs f
                                        :leftmost? (and (empty? leaves)
                                                        leftmost?))
                                 (dissoc :id))]
                (recur r 0 f (conj leaves new-leaf)))
              (recur r new-size cur-first leaves))))))
    [leaf]))

(defn update-leaf
  [{:keys [network dbid] :as leaf} idx t novelty remove-preds]
  (let [new-flakes (index/novelty-subrange leaf t novelty)
        to-remove  (filter-predicates remove-preds (:flakes leaf) new-flakes)]
    (if (or (seq new-flakes) (seq to-remove))
      (let [new-leaves (-> leaf
                           (dissoc :id)
                           (index/add-flakes new-flakes)
                           (index/rem-flakes to-remove)
                           rebalance-leaf)]
        (map (fn [l]
               (assoc l
                      :id (storage/random-leaf-id network dbid idx)
                      :t t))
             new-leaves))
      [leaf])))

(defn some-update-after?
  [t nodes]
  (->> nodes
       (map :t)
       (some (fn [node-t]
               (> t node-t))) ; t gets smaller as it moves forward!
       boolean))

(defn update-branch
  [{:keys [network dbid comparator], branch-t :t, :as branch} idx t child-nodes]
  (if (some-update-after? branch-t child-nodes)
    (let [children    (apply index/child-map comparator child-nodes)
          size        (->> child-nodes
                           (map :size)
                           (reduce +))
          first-flake (->> children first key)
          rhs         (->> children flake/last val :rhs)
          new-id      (storage/random-branch-id network dbid idx)]
      (assoc branch
             :id new-id
             :t t
             :children children
             :size size
             :first first-flake
             :rhs rhs))
    branch))

(defn rebalance-children
  [branch idx t child-nodes]
  (let [target-count (/ *overflow-children* 2)]
    (->> child-nodes
         (partition-all target-count)
         (map (fn [kids]
                (update-branch branch idx t kids)))
         (fn [[maybe-leftmost & not-leftmost]]
           (into [maybe-leftmost]
                 (map (fn [non-left-node]
                        (assoc non-left-node
                               :leftmost? false)))
                 not-leftmost)))))

(defn integrate-novelty
  "Returns a transducer that transforms a stream of index nodes in depth first
  order by incorporating the novelty flakes into the nodes, removing flakes with
  predicates in remove-preds, rebalancing the leaves so that none is bigger than
  *overflow-bytes*, and rebalancing the branches so that none have more children
  than *overflow-children*. Maintains a 'lifo' stack to preserve the depth-first
  order of the transformed stream."
  [idx t novelty remove-preds]
  (fn [xf]
    (let [stack (volatile! [])]
      (fn
        ;; Initialization: do nothing but initialize the nested transformer by
        ;; calling its initializing fn.
        ([]
         (xf))

        ;; Iteration:
        ;;   1. Incorporate each successive node with its corresponding novelty
        ;;      flakes.
        ;;   2. Rebalance both leaves and branches if they become too large after
        ;;      adding novelty by splitting them.
        ;;   3. Iterate each resulting node with the nested transformer.
        ([result node]
         (if (index/leaf? node)
           (let [leaves (update-leaf node idx t novelty remove-preds)]
             (vswap! stack into leaves)
             result)

           (loop [child-nodes []
                  stack*      @stack
                  result*     result]
             (let [child (peek stack*)]
               (if (and child
                        (index/descendant? node child)) ; all of a resolved
                                                        ; branch's children
                                                        ; should be at the top
                                                        ; of the stack
                 (recur (conj child-nodes child)
                        (vswap! stack pop)
                        (xf result* child))
                 (if (overflow-children? child-nodes)
                   (let [new-branches (rebalance-children node idx t child-nodes)
                         result**     (reduce xf result* new-branches)]
                     (recur new-branches
                            stack*
                            result**))
                   (let [branch (update-branch node idx t child-nodes)]
                     (vswap! stack conj branch)
                     result*)))))))

        ;; Completion: Flush the stack iterating each remaining node with the
        ;; nested transformer before calling the nested transformer's completion
        ;; fn on the iterated result.
        ([result]
         (loop [stack*  @stack
                result* result]
           (if-let [node (peek stack*)]
             (recur (vswap! stack pop)
                    (unreduced (xf result* node)))
             (xf result*))))))))

(defn resolve-novel-nodes
  "Returns a channel that will eventually contain a stream of index nodes
  descended from `root` in depth-first order. Only nodes with associated flakes
  from `novelty` will be resolved."
  [conn root novelty t remove-preds error-ch index-ch]
  (go
    (let [root-node (<! (resolve-if-novel conn root t novelty remove-preds
                                          error-ch))]
      (loop [stack [root-node]]
        (when-let [node (peek stack)]
          (let [stack* (pop stack)]
            (if (expanded? node)
              (do (>! index-ch (unmark-expanded node))
                  (recur stack*))
              (let [children (<! (resolve-children conn node t novelty remove-preds
                                                   error-ch))
                    stack**  (-> stack*
                                 (conj (mark-expanded node))
                                 (into (rseq children)))]
                (recur stack**))))))
      (async/close! index-ch)))
  index-ch)

(defn write-node
  "Writes `node` to storage, and puts any errors onto the `error-ch`"
  [conn idx {:keys [id network dbid] :as node} error-ch]
  (let [node         (dissoc node ::old-id)
        display-node (select-keys node [:id :network :dbid])]
    (go
      (try*
       (if (index/leaf? node)
         (do (log/debug "Writing index leaf:" display-node)
             (<? (storage/write-leaf conn network dbid idx id node)))
         (do (log/debug "Writing index branch:" display-node)
             (<? (storage/write-branch conn network dbid idx id node))))
       (catch* e
               (log/error e
                          "Error writing novel index node:" display-node)
               (>! error-ch e))))))

(defn write-resolved-nodes
  [conn network dbid idx error-ch index-ch]
  (go-loop [stats     {:idx idx, :novel 0, :unchanged 0, :garbage #{}}
            last-node nil]
    (if-let [{::keys [old-id] :as node} (<! index-ch)]
      (if (index/resolved? node)
        (let [written-node (<! (write-node conn idx node error-ch))
              stats*       (cond-> stats
                             (not= old-id :empty) (update :garbage conj old-id)
                             true                 (update :novel inc))]
          (recur stats*
                 written-node))
        (recur (update stats :unchanged inc)
               node))
      (assoc stats :root last-node))))

(defn refresh-index
  [conn network dbid error-ch {::keys [idx t novelty remove-preds root]}]
  (let [index-ch (async/chan 1 (integrate-novelty idx t novelty remove-preds))]
    (->> index-ch
         (resolve-novel-nodes conn root novelty t remove-preds error-ch)
         (write-resolved-nodes conn network dbid idx error-ch))))

(defn extract-root
  [{:keys [novelty t] :as db} remove-preds idx]
  (let [index-root    (get db idx)
        index-novelty (get novelty idx)]
    {::idx          idx
     ::root         index-root
     ::novelty      index-novelty
     ::t            t
     ::remove-preds remove-preds}))

(defn tally
  [db-status {:keys [idx root garbage]}]
  (-> db-status
      (update :db assoc idx root)
      (update :indexes conj idx)
      (update :garbage into garbage)))

(defn refresh-all
  ([db error-ch]
   (refresh-all db #{} error-ch))
  ([{:keys [conn network dbid] :as db} remove-preds error-ch]
   (->> index/types
        (map (partial extract-root db remove-preds))
        (map (partial refresh-index conn network dbid error-ch))
        async/merge
        (async/reduce tally {:db db, :indexes [], :garbage #{}}))))

(defn empty-novelty
  [db]
  (let [cleared (reduce (fn [db* idx]
                          (update-in db* [:novelty idx] empty))
                        db index/types)]
    (assoc-in cleared [:novelty :size] 0)))

(defn refresh
  ([db]
   (refresh db {:status "ready"}))
  ([{:keys [novelty block t network dbid] :as db}
    {:keys [ecount remove-preds]}]
   (go-try
     (let [start-time   (Instant/now)
           novelty-size (:size novelty)
           init-stats   {:network      network
                         :dbid         dbid
                         :t            t
                         :block        block
                         :novelty-size novelty-size
                         :start-time   start-time}]
       (if (or (dirty? db)
               (seq remove-preds))
         (do (log/info "Refreshing Index:" init-stats)
             (let [error-ch   (chan)
                   refresh-ch (refresh-all db remove-preds error-ch)]
               (async/alt!
                 error-ch
                 ([e]
                  (throw e))

                 refresh-ch
                 ([{:keys [garbage], refreshed-db :db, :as status}]
                  (let [indexed-db (-> refreshed-db
                                       empty-novelty
                                       (assoc-in [:stats :indexed] block))

                        block-file (storage/ledger-block-key network dbid block)]

                    ;; wait until confirmed writes before returning
                    (<? (storage/write-db-root indexed-db ecount))

                    ;; TODO - ideally issue garbage/root writes to RAFT together
                    ;;        as a tx, currently requires waiting for both
                    ;;        through raft sync
                    (<? (storage/write-garbage indexed-db garbage))
                    (let [end-time  (Instant/now)
                          duration  (- (.toEpochMilli ^Instant end-time)
                                       (.toEpochMilli ^Instant start-time))
                          end-stats (assoc init-stats
                                      :end-time end-time
                                      :duration duration)]
                      (log/info "Index refresh complete:" end-stats))
                    indexed-db)))))
         db)))))

(defn novelty-min-max
  "Returns a two-tuple of novelty min and novelty max given the session object"
  [session]
  (let [meta (-> session :conn :meta)]
    [(:novelty-min meta) (:novelty-max meta)]))


(defn novelty-min?
  "Returns true if ledger is beyond novelty-min threshold."
  [session db]
  (let [[min _] (novelty-min-max session)
        novelty-size (get-in db [:novelty :size])]
    (> novelty-size min)))


(defn novelty-max?
  "Returns true if ledger is beyond novelty-max threshold."
  [session db]
  (let [[_ max] (novelty-min-max session)
        novelty-size (get-in db [:novelty :size])]
    (> novelty-size max)))


(defn reindexed-db
  "If a db is being reindexed, this will return the db if it is complete, else nil."
  [session]
  (some-> (session/indexing-promise-ch session)
          async/poll!))


(defn do-index
  "Performs an index operation and returns a promise-channel of the latest db once complete"
  [session db remove-preds]
  (let [[lock? pc] (session/acquire-indexing-lock! session (async/promise-chan))]
    (when lock?
      ;; when we have a lock, reindex and put updated db onto pc.
      (async/go
        (let [indexed-db (<? (refresh db {:remove-preds remove-preds}))]
          (<? (txproto/write-index-point-async (-> db :conn :group) indexed-db))
          ;; updated-db might have had additional block(s) written to it, so instead
          ;; of using it, reload from disk.
          (session/clear-db! session)                       ;; clear db cache to force reload
          (async/put! pc indexed-db))))
    pc))


(defn merge-new-index
  "Attempts to merge newly completed index job with block-map.
  Optimistically the new index will be one block behind the block-map,
  but if not we will pause momentarily to try to see if things catch up
  before throwing an error."
  ([session block-map reindexed-db]
   (merge-new-index session block-map reindexed-db 0))
  ([session {:keys [flakes block] :as block-map} reindexed-db retries]
   (go-try
     (let [max-retries           20
           pause-before-retry-ms 100
           indexed-block         (:block reindexed-db)
           current?              (= indexed-block (dec block))]
       (cond
         ;; everything is good, re-associate db-after with reindexed db
         current?
         (let [updated-db (<? (dbproto/-with reindexed-db block flakes))]
           (session/release-indexing-lock! session (:block updated-db))
           (assoc block-map :db-after updated-db))

         (= retries max-retries)
         (throw (ex-info (str "Recently re-indexed DB on disk is not current after " max-retries " retries. "
                              "Pending new block: " block ", latest block on disk: " (:block reindexed-db) ".")
                         {:status 500 :error :db/unexpected-error}))

         :else
         (let [_          (async/<! (async/timeout pause-before-retry-ms))
               updated-db (<? (session/current-db session))]
           (<? (merge-new-index session block-map updated-db (inc retries)))))))))


(defn novelty-max-block
  "When at novelty-max, we need to stop everything until we can get an updated index."
  [session block-map]
  (go-try
    (if-let [indexing-ch (session/indexing-promise-ch session)]
      ;; existing indexing process happening, wait until complete
      (<? (merge-new-index session block-map (<? indexing-ch)))
      ;; indexing not yet happening, initiate it on original db (pre-block)
      ;; so long as the last index isn't also pre-block, in which case proceed.
      (let [db             (:db-orig block-map)
            index-current? (= (:block db) (dec (:block block-map)))]
        (if index-current?
          block-map
          (let [updated-orig (<? (do-index session db nil))
                block-map*   (assoc block-map :db-orig updated-orig)]
            (<? (merge-new-index session block-map* updated-orig))))))))


(defn ensure-indexing
  "Checks if indexing operation is happening, and if not kicks one off."
  [session block-map]
  (let [indexing? (some? (session/indexing-promise-ch session))]
    (when-not indexing?
      (do-index session (:db-after block-map) (:remove-preds block-map)))))

;;; =====================================
;;;
;;; Maintenance Utilities
;;;
;;; =====================================


(defn validate-idx-continuity
  "Checks continuity of provided index in that the 'rhs' is equal to the
  first-flake of the following segment."
  ([conn idx-root] (validate-idx-continuity idx-root false))
  ([conn idx-root throw?] (validate-idx-continuity idx-root throw? nil))
  ([conn idx-root throw? cmp]
   (let [node     (async/<!! (index/resolve conn idx-root))
         children (:children node)
         last-i   (dec (count children))]
     (println "Idx children: " (inc last-i))
     (loop [i        0
            last-rhs nil]
       (let [child       (-> children (nth i) val)
             resolved    (async/<!! (index/resolve conn child))
             {:keys [id rhs leftmost?]} child
             child-first (:first child)
             resv-first  (first (:flakes resolved))
             ;; If first-flake is deleted, it should STILL be the first/rhs for
             ;; unresolved nodes to maintain continuity
             continuous? (= last-rhs child-first)]
         (println "->>" id)
         (println "   first-flake: " child-first)
         (println "    first-resv: " resv-first)
         (println "      last-rhs: " last-rhs)
         (println "     leftmost?: " leftmost?)
         (println "           rhs: " rhs)
         (when (and cmp
                    child-first
                    rhs)
           (println "         comp: " (cmp child-first rhs)))
         (when (and throw?
                    (not (zero? i))
                    (not continuous?))
           (throw (Exception. (str "NOT CONTINUOUS!!!: " (pr-str {:id             id
                                                                  :idx            i
                                                                  :last-rhs       last-rhs
                                                                  :first          child-first
                                                                  :first-resolved resv-first
                                                                  :rhs            rhs
                                                                  :leftmost?      leftmost?})))))
         (if (= i last-i)
           (println "Done validating idx-continuity")
           (recur (inc i) (:rhs child))))))))


(comment

  (def conn (:conn user/system))

  (def db (async/<!! (fluree.db.api/db conn "test/two")))

  (defn check-ctnty
    [{:keys [conn] :as db}]
    (let [spot-comp (.comparator (-> db :novelty :spot))
          post-comp (.comparator (-> db :novelty :post))
          psot-comp (.comparator (-> db :novelty :psot))
          opst-comp (.comparator (-> db :novelty :opst))
          tspo-comp (.comparator (-> db :novelty :tspo))]
      (do (validate-idx-continuity conn (:spot db) true spot-comp)
          (validate-idx-continuity conn (:post db) true post-comp)
          (validate-idx-continuity conn (:psot db) true psot-comp)
          (validate-idx-continuity conn (:opst db) true opst-comp)
          (validate-idx-continuity conn (:tspo db) true tspo-comp))))

  (check-ctnty db)

  (defn add-users-txn
    [x n]
    (mapv #(hash-map :_id "_user" :username (str "#(str \"" x "\" (+ (now) " % "))"))
          (range 1 (inc n))))

  (def ids-to-delete (range (- 87960930233080 10000) 87960930233080))

  (defn delete-users-txn
    [ids]
    (mapv #(hash-map :_id % :_action "delete") ids))

  (def res (async/<!! (fluree.db.api/transact-async conn "fluree/test" (add-users-txn "a" 10000))))

  res

  (def delete-res (async/<!! (fluree.db.api/transact-async conn "test/two" (delete-users-txn ids-to-delete))))

  (loop [n 20]
    (if (> n 0)
      (do (async/<!! (fluree.db.api/transact-async conn "smol/one" (add-users-txn n 1000)))
          (recur (dec n)))
      true))

  delete-res

  (def spot-comp (.comparator (-> db :novelty :spot)))

  (validate-idx-continuity (:spot db) true spot-comp))
