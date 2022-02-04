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
  [child-map]
  (> (count child-map) *overflow-children*))

(defn add-garbage
  "Adds the `:id` of the outdated index node second argument to the `garbage`
  list only if the id is not `:empty`."
  [garbage {id :id}]
  (cond-> garbage
          (not= :empty id) (conj id)))

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

(defn except-preds
  [flakes preds]
  (->> flakes
       (filter (fn [f]
                 (contains? preds (flake/p f))))
       (flake/disj-all flakes)))

(defn update-node
  [node t novelty remove-preds]
  (if (index/leaf? node)
    (-> node
        (index/at-t t novelty)
        (update :flakes except-preds remove-preds))
    (let [cmp         (:comparator node)
          novel-first (-> node
                          (index/novelty-subrange t novelty)
                          first)]
      (update node :first (fn [f]
                            (if (neg? (cmp f novel-first))
                              f
                              novel-first))))))

(defn resolve-if-novel
  "Resolves data associated with `node` from storage configured in the connection
  `conn` if the `novelty` set contains flakes within the range of node or the
  `remove-preds` sequence is nonempty. Any errors are put on the `error-ch`"
  [conn node t novelty remove-preds error-ch]
  (go
    (let [node-novelty (index/novelty-subrange node t novelty)]
      (if (or (seq node-novelty) (seq remove-preds))
        (try* (let [resolved-node (<? (index/resolve conn node))]
                (update-node resolved-node t novelty remove-preds))
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

(defn resolve-tree
  "Returns a channel that will eventually contain a stream of index nodes in
  depth-first order. Only nodes with associated flakes from `index-novelty` will
  be resolved."
  [conn index-root index-novelty t remove-preds error-ch]
  (let [tree-ch (async/chan 4)
        stat-ch (async/chan 1)]
    (go
      (let [root-node (<! (resolve-if-novel conn index-root t index-novelty
                                            remove-preds error-ch))]
        (loop [stack [root-node]
               stats {:novel 0, :unchanged 0, :stale []}]
          (if (empty? stack)
            (do (>! stat-ch stats)
                (async/close! stat-ch)
                (async/close! tree-ch))
            (let [node   (peek stack)
                  stack* (pop stack)]
              (if (expanded? node)
                (let [stats* (if (index/resolved? node)
                               (-> stats
                                   (update :novel inc)
                                   (update :stale add-garbage node))
                               (update stats :unchanged inc))]
                  (>! tree-ch node)
                  (recur stack* stats*))
                (let [children (<! (resolve-children conn node t index-novelty remove-preds error-ch))
                      stack**  (-> stack*
                                   (conj (mark-expanded node))
                                   (into (rseq children)))]
                  (recur stack** stats))))))))
    [tree-ch stat-ch]))

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

(def rebalance-leaf-xf
  (mapcat (fn [node]
            (if (index/leaf? node)
              (rebalance-leaf node)
              [node]))))

(defn rebalance-leaves
  [node-stream]
  (let [rebalance-ch (async/chan 1 rebalance-leaf-xf)]
    (async/pipe node-stream rebalance-ch)))

(defn rebalance-children
  "Splits branch nodes if they have more than `*overflow-children*` child nodes."
  [parent children]
  (let [target-count (/ *overflow-children* 2)]
    (loop [new-branches []
           remaining    children]
      (if (> (count remaining) target-count)
        (let [[new-children rst-children]
              (avl/split-at target-count remaining)

              first-flake (-> new-children first key)
              rhs         (-> rst-children first key)
              new-branch  (-> parent
                              (dissoc :id)
                              (assoc :children new-children
                                     :first first-flake
                                     :rhs rhs))]
          (recur (conj new-branches new-branch) rst-children))
        (let [first-flake (-> remaining first key)
              last-child  (-> parent
                              (dissoc :id)
                              (assoc :children remaining
                                     :first first-flake))]
          (conj new-branches last-child))))))

(defn reconcile-leaf-size
  [{:keys [flakes] :as leaf}]
  (let [total-size (->> flakes
                        (map flake/size-flake)
                        (reduce +))]
    (assoc leaf :size total-size)))

(defn reconcile-branch-size
  [{:keys [children] :as branch}]
  (let [total-size (->> children
                        vals
                        (map :size)
                        (reduce +))]
    (assoc branch :size total-size)))

(defn write-if-novel
  "Writes `node` to storage if it has been updated, and puts any errors onto the
  `error-ch`"
  [conn network dbid idx error-ch node]
  (go
    (if (index/resolved? node)
      (try* (if (index/leaf? node)
              (let [leaf (reconcile-leaf-size node)]
                (log/debug "Writing index leaf:"
                           (select-keys leaf [:id :network :dbid]))
                (<? (storage/write-leaf conn network dbid idx leaf)))
              (let [branch (reconcile-branch-size node)]
                (log/debug "Writing index branch:"
                           (select-keys branch [:id :network :dbid]))
                (<? (storage/write-branch conn network dbid idx branch))))
            (catch* e
              (log/error e
                         "Error writing novel index node:"
                         (select-keys node [:id :network :dbid]))
              (>! error-ch e)))
      node)))

(defn write-child-nodes
  [conn network dbid idx error-ch parent child-nodes]
  (let [cmp (:comparator parent)]
    (->> child-nodes
         (map (fn [child]
                (write-if-novel conn network dbid idx error-ch child)))
         (async/map (fn [& written-nodes]
                      (apply index/child-map cmp written-nodes))))))

(defn write-descendants
  "Writes the `descendants` of the branch node `parent`, adding new branch levels
  if any branch node is too large"
  [conn network dbid idx error-ch parent descendants]
  (go-loop [children (<! (write-child-nodes conn network dbid idx error-ch parent descendants))]
    (if (overflow-children? children)
      (do (log/debug "Rebalancing index branch:"
                     (select-keys parent [:id :network :dbid]))
          (let [child-branches (rebalance-children parent children)]
            (recur (<! (write-child-nodes conn network dbid idx error-ch parent child-branches)))))
      children)))

(defn descendant?
  [{:keys [rhs leftmost?], cmp :comparator, first-flake :first, :as branch}
   {node-first :first, node-rhs :rhs, :as node}]
  (if-not (index/branch? branch)
    false
    (and (or leftmost?
             (not (pos? (cmp first-flake node-first))))
         (or (nil? rhs)
             (and (not (nil? node-rhs))
                  (not (pos? (cmp node-rhs rhs))))))))

(defn pop-descendants
  "Pops all the descendants of `branch` off of the top of the stack `in-stack`"
  [branch in-stack]
  (loop [child-nodes []
         stack       in-stack]
    (let [nxt (peek stack)]
      (if (and nxt (descendant? branch nxt))
        (recur (conj child-nodes nxt) (pop stack))
        [child-nodes stack]))))

(defn write-tree
  [conn network dbid idx error-ch node-stream]
  (let [out (async/chan)]
    (go-loop [stack []]
      (if-let [node (<! node-stream)]
        (if (index/leaf? node)
          (recur (conj stack node))
          (let [;; all descendants of a branch node should be at the top of the
                ;; stack as long as the `node-stream` is in depth-first order
                [descendants stack*] (pop-descendants node stack)
                children    (<! (write-descendants conn network dbid idx error-ch
                                                   node descendants))
                first-flake (-> children first key)
                branch      (-> node
                                unmark-expanded
                                (dissoc :id)
                                (assoc :first first-flake
                                       :children children))]
            (recur (conj stack* branch))))
        (async/pipe (write-if-novel conn network dbid idx error-ch (peek stack))
                    out)))
    out))

(defn tally
  [idx index-ch stat-ch]
  (go
    (let [new-root (<! index-ch)
          stats    (<! stat-ch)]
      (assoc stats :idx idx, :root new-root))))

(defn refresh-root
  [conn network dbid error-ch {::keys [idx root novelty t remove-preds]}]
  (let [[tree-ch stat-ch] (resolve-tree conn root novelty t remove-preds error-ch)
        index-ch (->> tree-ch
                      rebalance-leaves
                      (write-tree conn network dbid idx error-ch))]
    (tally idx index-ch stat-ch)))

(defn extract-root
  [{:keys [novelty t] :as db} remove-preds idx]
  (let [index-root    (get db idx)
        index-novelty (get novelty idx)]
    {::idx          idx
     ::root         index-root
     ::novelty      index-novelty
     ::t            t
     ::remove-preds remove-preds}))

(defn update-refresh-status
  [db-status {:keys [idx root stale]}]
  (-> db-status
      (update :db assoc idx root)
      (update :indexes conj idx)
      (update :stale into stale)))

(defn refresh-all
  ([db error-ch]
   (refresh-all db #{} error-ch))
  ([{:keys [conn network dbid] :as db} remove-preds error-ch]
   (->> index/types
        (map (partial extract-root db remove-preds))
        (map (partial refresh-root conn network dbid error-ch))
        async/merge
        (async/reduce update-refresh-status {:db db, :indexes [], :stale []}))))

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
                 ([{:keys [stale], refreshed-db :db, :as status}]
                  (let [indexed-db (-> refreshed-db
                                       empty-novelty
                                       (assoc-in [:stats :indexed] block))

                        block-file (storage/ledger-block-key network dbid block)]

                    ;; wait until confirmed writes before returning
                    (<? (storage/write-db-root indexed-db ecount))

                    ;; TODO - ideally issue garbage/root writes to RAFT together
                    ;;        as a tx, currently requires waiting for both
                    ;;        through raft sync
                    (<? (storage/write-garbage indexed-db stale))
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
