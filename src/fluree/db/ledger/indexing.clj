(ns fluree.db.ledger.indexing
  (:require [clojure.data.avl :as avl]
            [clojure.tools.logging :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.index :as index]
            [fluree.db.storage.core :as storage]
            [fluree.db.session :as session]
            [clojure.core.async :as async :refer [>! <! chan go go-loop]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto])
  (:import (fluree.db.flake Flake)
           (java.time Instant)))


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
  "Adds item to the garbage key within progress"
  [idx-key leaf? progress-atom]
  ;; special case where brand new db has :empty as id before first index an
  ;; already resolved IndexNode will not have an :id (idx-key will be null) -
  ;; happens with blank-db
  (when (and idx-key (not= :empty idx-key))
    (swap! progress-atom update :garbage (fn [g]
                                           (cond-> (conj g idx-key)
                                             ;; if at leaf, add history node too
                                             leaf? (conj (str idx-key "-his")))))))

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

(defn mark-novel
  "Mark that there are flakes in novelty within the subrange of the index node map
  `node`"
  [node]
  (assoc node ::novel true))

(defn novel?
  "Returns true if the index node map `node` was marked novel because there were
  flakes in novelty within the subrange of `node` when it was resolved"
  [node]
  (-> node ::novel true?))

(defn mark-unchanged
  "Mark that there are not any flakes in novelty within the subrange of the index
  node map `node`"
  [node]
  (assoc node ::novel false))

(defn unchanged?
  "Returns true if the index node map `node` was not marked novel because there
  weren't any flakes in novelty within the subrange of `node` when it was
  resolved"
  [node]
  (-> node ::novel false?))

(defn mark-expanded
  [node]
  (assoc node ::expanded true))

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

(defn novel-node-xf
  [t novelty remove-preds]
  (comp (map (fn [node]
               (if (index/leaf? node)
                 (-> node
                     (index/at-t t novelty)
                     (update :flakes except-preds remove-preds))
                 node)))
        (map mark-novel)))

(defn resolve-if-novel
  ""
  [conn node t novelty remove-preds]
  (let [node-novelty (index/novelty-subrange node t novelty)]
    (if (or (seq node-novelty) (seq remove-preds))
      (let [novel-ch (async/chan 1 (novel-node-xf t novelty remove-preds))]
        (-> (dbproto/resolve conn node)
            (async/pipe novel-ch)))
      (let [unchanged-ch (async/chan 1 (map mark-unchanged))]
        (async/put! unchanged-ch node)
        unchanged-ch))))

(defn resolve-children
  "Resolves a branch's children in parallel, and loads data for each child only if
  there are novelty flakes associated with that child. Returns a channel that
  will eventually contain a vector of resolved children."
  [conn branch t novelty remove-preds]
  (->> branch
       :children
       (map (fn [[_ child]]
              (resolve-if-novel conn child t novelty remove-preds)))
       (async/map vector)))

(defn resolve-tree
  [{:keys [conn novelty block t network dbid] :as db} idx remove-preds]
  (let [index-root   (get db idx)
        novelty-root (get novelty idx)
        tree-ch      (async/chan 4)
        stat-ch      (async/chan 1)]
    (go
      (let [root-node (<! (resolve-if-novel conn index-root t novelty-root
                                            remove-preds))]
        (loop [stack [root-node]
               stats {:idx idx, :novel 0, :unchanged 0, :stale []}]
          (if (empty? stack)
            (do (async/put! stat-ch stats)
                (async/close! tree-ch))
            (let [node   (peek stack)
                  stack* (pop stack)]
              (if (expanded? node)
                (let [stats* (if (novel? node)
                               (-> stats
                                   (update :novel inc)
                                   (update :stale conj (:id node)))
                               (update stats :unchanged inc))]
                  (>! tree-ch node)
                  (recur stack* stats*))
                (let [children (<? (resolve-children conn node t novelty-root remove-preds))
                      stack**  (-> stack*
                                   (conj (mark-expanded node))
                                   (into (rseq children)))]
                (recur stack** stats))))))))
    [tree-ch stat-ch]))

(defn rebalance-leaf
  [{:keys [flakes leftmost? ciel] :as leaf}]
  (let [target-size (/ *overflow-bytes* 2)]
    (loop [[f & r]   flakes
           cur-size  0
           cur-first f
           leaves    []]
      (if (empty? r)
        (let [subrange  (flake/subrange flakes >= cur-first)
              last-leaf (-> leaf
                            (assoc :flakes subrange
                                   :floor cur-first
                                   :ciel ciel)
                            (dissoc :id :leftmost?))]
          (conj leaves last-leaf))
        (let [new-size (-> f flake/size-flake (+ cur-size))]
          (if (> new-size target-size)
            (let [subrange (flake/subrange flakes >= cur-first < f)
                  new-leaf (-> leaf
                               (assoc :flakes subrange
                                      :floor cur-first
                                      :ciel f
                                      :leftmost? (and (empty? leaves)
                                                      leftmost?))
                               (dissoc :id))]
              (recur r 0 f (conj leaves new-leaf)))
            (recur r new-size cur-first leaves)))))))

(def rebalance-leaf-xf
  (mapcat (fn [node]
            (if (and (index/leaf? node)
                     (overflow-leaf? node))
              (rebalance-leaf node)
              [node]))))

(defn rebalance-leaves
  [node-stream]
  (let [rebalance-ch (async/chan 1 rebalance-leaf-xf)]
    (async/pipe node-stream rebalance-ch)))

(defn rebalance-children
  [parent children]
  (let [target-count (/ *overflow-children* 2)]
    (loop [new-branches []
           remaining    children]
      (if (> (count remaining) target-count)
        (let [[new-children rst-children]
              (avl/split-at target-count remaining)

              floor      (-> new-children first key)
              ciel       (-> rst-children first key)
              new-branch (-> parent
                             (dissoc :id)
                             (assoc :children new-children
                                    :floor    floor
                                    :ciel     ciel))]
          (recur (conj new-branches new-branch) rst-children))
        (let [floor      (-> remaining first key)
              last-child (-> parent
                             (dissoc :id)
                             (assoc :children remaining
                                    :floor    floor))]
          (conj new-branches last-child))))))

(defn write-if-novel
  [{:keys [conn network dbid] :as db} idx node]
  (if (novel? node)
    (if (index/leaf? node)
      (storage/write-leaf conn network dbid idx node)
      (storage/write-branch conn network dbid idx node))
    (let [out (async/chan)]
      (async/put! out node)
      out)))

(defn write-child-nodes
  [db idx parent child-nodes]
  (let [cmp (:comparator parent)]
    (->> child-nodes
         (map (fn [child]
                (write-if-novel db idx child)))
         (async/map (fn [& written-nodes]
                      (apply index/child-map cmp written-nodes))))))

(defn write-decendants
  [db idx parent decendants]
  (go-loop [children (<! (write-child-nodes db idx parent decendants))]
    (if (overflow-children? children)
      (let [child-branches (rebalance-children parent children)]
        (recur (<! (write-child-nodes db idx parent child-branches))))
      children)))

(defn descendant?
  [{:keys [floor ciel] :as branch} node]
  (if-not (index/branch? branch)
    false
    (let [cmp (:comparator branch)
          {node-floor :floor, node-ciel :ciel} node]
      (and (not (pos? (cmp floor node-floor)))
           (or (nil? ciel)
               (and (not (nil? node-ciel))
                    (not (pos? (cmp node-ciel ciel)))))))))

(defn pop-decendants
  [{:keys [floor ciel] :as branch} in-stack]
  (loop [child-nodes []
         stack       in-stack]
    (let [nxt (peek stack)]
      (if (and nxt (descendant? branch nxt))
        (recur (conj child-nodes nxt) (pop stack))
        [child-nodes stack]))))

(defn write-tree
  [db idx node-stream]
  (let [out (async/chan)]
    (go-loop [stack []]
      (if-let [node (<! node-stream)]
        (if (index/leaf? node)
          (recur (conj stack node))
          (let [[decendants stack*]
                (pop-decendants node stack)

                children (<! (write-decendants db idx node decendants))
                floor    (-> children first key)
                branch   (-> node
                             (dissoc :id)
                             (assoc :floor    floor
                                    :children children))]
            (recur (conj stack* branch))))
        (async/pipe (write-if-novel db idx (peek stack))
                    out)))
    out))

(defn tally
  [index-ch stat-ch]
  (go
    (let [new-root (<! index-ch)
          stats    (<! stat-ch)]
      (assoc stats :root new-root))))

(defn refresh-root
  [{:keys [conn novelty block t network dbid] :as db} remove-preds idx]
  (let [[tree-ch stat-ch] (resolve-tree db idx remove-preds)
        index-ch          (->> tree-ch
                               rebalance-leaves
                               (write-tree db idx))]
    (tally index-ch stat-ch)))

(defn update-refresh-status
  [db-status {:keys [idx root stale]}]
  (-> db-status
      (update :db assoc idx root)
      (update :indexes conj idx)
      (update :stale into stale)))

(defn refresh-all
  ([db]
   (refresh-all db #{}))
  ([db remove-preds]
   (->> index/types
        (map (partial refresh-root db remove-preds))
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
      (log/info "Refreshing Index:" init-stats)
      (if (or (dirty? db)
              (seq remove-preds))
        (let [{:keys [indexes stale] :as status}
              (<! (refresh-all db remove-preds))

              refreshed-db (:db status)

              indexed-db   (-> refreshed-db
                               empty-novelty
                               (assoc-in [:stats :indexed] block))]

          ;; wait until confirmed writes before returning
          ;; TODO - ideally issue garbage/root writes to RAFT together as a tx,
          ;;        currently requires waiting for both through raft sync
          (<? (storage/write-garbage indexed-db stale))
          (<? (storage/write-db-root indexed-db ecount))
          (let [end-time  (Instant/now)
                duration  (- (.toEpochMilli end-time)
                             (.toEpochMilli start-time))
                end-stats (assoc init-stats
                                 :end-time end-time
                                 :duration duration)]
            (log/info "Index refresh complete:" end-stats))
          indexed-db)
        db)))))

(defn novelty-min
  "Given a db session, returns minimum novelty threshold for reindexing."
  [session]
  (-> session :conn :meta :novelty-min))

;; TODO - should track new index segments and if failure, garbage collect them
(defn index*
  ([session {:keys [remove-preds] :as opts}]
   (go-try
    (if (session/indexing? session)
      false
      (let [latest-db     (<? (session/current-db session))
            novelty-size  (get-in latest-db [:novelty :size])
            novelty-min   (novelty-min session)]
        (if (or (>= novelty-size novelty-min)
                (seq remove-preds))
          (<? (index* session latest-db opts))
          false)))))
  ([session {:keys [conn block network dbid] :as db} opts]
   (go-try
    (let [last-index (session/indexed session)]
      (cond
        (and last-index (<= block last-index))
        (do
          (log/info "Index called on DB but last index isn't older."
                    {:last-index last-index
                     :block      block
                     :db         (pr-str db)
                     :session    (pr-str session)})
          false)

        (session/acquire-indexing-lock! session block)
        (let [updated-db (<? (refresh db opts))
              group      (-> updated-db :conn :group)]
          (<? (txproto/write-index-point-async group updated-db))
          (session/clear-db! session)                      ;; clear db cache to force reload
          (session/release-indexing-lock! session block)   ;; free up our lock
          (<? (index* session opts))                       ;; run a new index check in case we need to start another one immediately
          true)

        :else
        (do
          (log/warn "Indexing process failed to obtain index lock. Extremely Unusual."
                    {:network network :db (pr-str db) :block block :indexing? (session/indexing? session)})
          false))))))

;;; =====================================
;;;
;;; Maintenance Utilities
;;;
;;; =====================================


(defn validate-idx-continuity
  "Checks continuity of provided index in that the 'ciel' is equal to the
  floor of the following segment."
  ([conn idx-root] (validate-idx-continuity idx-root false))
  ([conn idx-root throw?] (validate-idx-continuity idx-root throw? nil))
  ([conn idx-root throw? compare]
   (let [node     (async/<!! (dbproto/resolve conn idx-root))
         children (:children node)
         last-i   (dec (count children))]
     (println "Idx children: " (inc last-i))
     (loop [i        0
            last-ciel nil]
       (let [child       (-> children (nth i) val)
             resolved    (async/<!! (dbproto/resolve conn child))
             {:keys [id ciel leftmost?]} child
             child-floor (:floor child)
             resv-floor  (first (:flakes resolved))
             ;; If floor is deleted, it should STILL be the first/ciel
             ;; for unresolved nodes to maintain continuity
             continuous? (= last-ciel child-floor)]
         (println "->>" id)
         (println "         floor: " child-floor)
         (println "    floor-resv: " resv-floor)
         (println "      last-ciel: " last-ciel)
         (println "     leftmost?: " leftmost?)
         (println "           ciel: " ciel)
         (when (and compare
                    child-floor ciel)
           (println "         comp: " (compare child-floor ciel)))
         (when (and throw?
                    (not (zero? i))
                    (not continuous?))
           (throw (Exception. (str "NOT CONTINUOUS!!!: " (pr-str {:id             id
                                                                  :idx            i
                                                                  :last-ciel      last-ciel
                                                                  :floor          child-floor
                                                                  :floor-resolved resv-floor
                                                                  :ciel           ciel
                                                                  :leftmost?      leftmost?})))))
         (if (= i last-i)
           (println "Done validating idx-continuity")
           (recur (inc i) (:ciel child))))))))


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
