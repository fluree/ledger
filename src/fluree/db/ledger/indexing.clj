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
(defn overflow?
  [size-bytes]
  (> size-bytes *overflow-bytes*))

(def ^:dynamic *underflow-bytes* 50000)
(defn underflow?
  [size-bytes]
  (< size-bytes *underflow-bytes*))

(def ^:dynamic *overflow-children* 500)
(defn overflow-children?
  [child-count]
  (> child-count *overflow-children*))

(defn add-garbage
  "Adds item to the garbage key within progress"
  [idx-key leaf? progress-atom]
  ;; special case where brand new db has :empty as id before first index
  ;; an already resolved IndexNode will not have an :id (idx-key will be null) - happens with blank-db
  (when (and idx-key (not= :empty idx-key))
    (swap! progress-atom update :garbage (fn [g]
                                           (cond-> (conj g idx-key)
                                                   ;; if at leaf, add history node too
                                                   leaf? (conj (str idx-key "-his")))))))

(defn dirty?
  "Returns `true` if the index for `db` of type `idx` is out of date, or if `db` has
  any out of date index if `idx` is unspecified. Returns `false` otherwise."
  ([db idx]
   (-> db
       :novelty
       (get idx)
       seq
       boolean))
  ([db]
   (->> index/types
        (map (partial dirty? db))
        some
        boolean)))

(defn mark-novel
  "Mark that the index node map `node` has been changed from it's resolved value"
  [node]
  (assoc node ::novel true))

(defn mark-unchanged
  "Mark that the index node map `node` has not been changed from it's resolved
  value"
  [node]
  (assoc node ::novel false))

(defn unresolved?
  "Returns true if the index node map `node` has not had all of it's referenced
  data loaded from storage"
  [node]
  (-> node ::novel nil?))

(defn novel?
  "Returns true if the index node map `node` has been updated since it's reference
  data was last stored"
  [node]
  (-> node ::novel true?))

(defn unchanged?
  "Returns true if the index node map `node` has not been updated since it's
  reference data was last stored"
  [node]
  (-> node ::novel false?))

(defn mark-expanded
  [branch]
  (assoc node ::expanded true))

(defn expanded?
  (or (index/leaf? node)
      (-> node ::expanded true?)))

(defn resolve-if-novel
  [conn node t novelty remove-preds]
  (if (unresolved? node)
    (let [node-novelty (index/node-subrange node t novelty)]
      (if (or (seq node-novelty)
              (seq remove-preds))
        (go
          (let [res (<? (dbproto/resolve conn node))]
            (cond-> res
              (index/leaf? res) (index/value-at-t t node-novelty remove-preds)
              :finally mark-novel)))
        (let [out (async/chan)]
          (->> node
               mark-unchanged
               (async/put! out))
          out)))
    node))

(defn resolve-children
  "Resolves a branch's children in parallel, and loads data for each child only if
  there are novelty flakes associated with that child. The resolved children's
  order is reversed so they have the correct order coming off of a lifo resolve
  stack"
  [conn branch t novelty remove-preds]
  (defn [reverse-chan (async/chan 1 (map rseq))])
  (->> branch
       :children
       (mapv (fn [[_ child]]
               (resolve-if-novel conn child t novelty remove-preds)))
       (async/map vec)
       (async/pipe reverse-chan)))

(defn resolve-tree
  [{:keys [conn novelty block t network dbid] :as db} idx remove-preds]
  (go
    (let [index-root   (get db idx)
          root-node    (<? (resolve-if-novel conn index-root t novelty-root remove-preds))
          novelty-root (get novelty idx)]
      (loop [stack  [root-node]
             result []]
        (if (empty? stack)
          result
          (let [node   (peek stack)
                stack* (pop stack)]
            (if (expanded? node)
              (recur stack* (conj result node))
              (let [children (<? (resolve-children conn node t novelty-root remove-preds))
                    stack**  (-> stack*
                                 (conj (mark-expanded node))
                                 (into children))]
                (recur stack** result)))))))))

(defn rebalance-leaf
  [{:keys [flakes leftmost?] :as leaf}]
  (if (-> flakes flake/size-bytes overflow?)
    (let [target-size (/ *overflow-bytes* 2)]
      (loop [[f & r]   flakes
             cur-size  0
             cur-first f
             leaves    []]
        (if (empty? r)
          leaves
          (let [new-size (-> f flake/size-flake (+ cur-size))]
            (if (> new-size target-size)
              (let [subrange (flake/subrange flakes >= cur-first < f)
                    new-leaf (-> leaf
                                 (assoc :flakes subrange
                                        :first-flake cur-first
                                        :rhs f
                                        :leftmost? (and (empty? leaves)
                                                        leftmost?))
                                 (dissoc :id))]
                (recur r 0 f (conj leaves new-leaf)))
              (recur r new-size cur-first leaves))))))
    [leaf]))

(defn rebalance-branch
  [{:keys [children] :as branch}]
  (let [child-count (count children)]
    (if (overflow-children? child-count)
      (let [target-count (int (Math/ceil (/ *overflow-children* 2)))]
        (loop [new-branches (transient (empty children))
               remaining    children]
          (if (> (count remaining) target-count)
            (let [[child-map rst-map]
                  (avl/split-at target-count remaining)

                  split-point (nth remaining (inc target-count))
                  first-flake (-> child-map first key)
                  new-branch  (assoc branch
                                     :children    child-map
                                     :first-flake first-flake
                                     :rhs         split-point)
                  new-entry   (index/child-entry new-branch)]
              (recur (conj! new-branches new-entry) rst-map))
            (let [first-flake (-> remaining first key)
                  rhs         (:rhs branch)
                  last-child  (assoc branch
                                     :children    remaining
                                     :first-flake first-flake
                                     :rhs         rhs)]
              (assoc branch
                     :children (-> new-branches
                                   (conj! last-child)
                                   persistent!))))))
      branch)))

(defn rebalance-tree
  [cmp tree]
  (loop [[node & rst] tree
         result       (transient (index/child-map cmp))]
    (if-not node
      (persistent! result)
      (if (index/leaf? node)
        (let [balanced-leaves (rebalance-leaf node)
              result*         (reduce (fn [res {:keys [first-flake] :as leaf}]
                                        (assoc! res first-flake leaf))
                                      result balanced-leaves)]
          (recur rst result*))
        (let [{:keys [first-flake rhs]} node
              children     (if rhs
                             (flake/subrange result >= first-flake < rhs)
                             (flake/subrange result >= first-flake))
              branch-entry (-> node
                               (assoc :children children)
                               rebalance-branch
                               index/child-entry)
              result*      (reduce (fn [res child]
                                     (disj! res child))
                                   result children)]
          (recur rst (conj! result* branch-entry)))))))

(def merge-with-plus
  (partial merge-with +))

(defn update-refresh-status
  [db-status refresh-status]
  (let [{:keys [index root branches leaves stale]}
        refresh-status]
    (-> db-status
        (update :db assoc index root)
        (update :indexes conj index)
        (update :branches merge-with-plus branches)
        (update :leaves merge-with-plus leaves)
        (update :stale into stale))))

(defn empty-index-novelty
  [db idx]
  (update-in db [:novelty idx] empty))

(defn empty-novelty
  [db]
  (let [cleared (reduce empty-index-novelty db index/types)]
    (assoc-in cleared [:novelty :size] 0)))

   (go-try
    (let [start-time   (Instant/now)
          novelty-size (:size novelty)]
      (log/info (str "Index Update begin at: " start-time)
                {:network      network
                 :dbid         dbid
                 :t            t
                 :block        block
                 :novelty-size novelty-size})
      (if (or (dirty? db)
              (seq remove-preds))
        (let [{:keys [index branches leaves stale] :as status}
              (<! (refresh-all db))

              refreshed-db (:db status)

              indexed-db   (-> refreshed-db
                               empty-novelty
                               (assoc-in [:stats :indexed] block))]
          ;; wait until confirmed writes before returning
          ;; TODO - ideally issue garbage/root writes to RAFT together as a tx, currently requires waiting for both through raft sync
          (<? (storage/write-garbage indexed-db stale))
          (<? (storage/write-db-root indexed-db ecount))
          (log/info (str "Index Update end at: " (Instant/now))
                    {:network  network
                     :dbid     dbid
                     :block    block
                     :t        t
                     :duration (- (.toEpochMilli (Instant/now))
                                  (.toEpochMilli start-time))})
          indexed-db)
        db)))))

;; TODO - should track new index segments and if failure, garbage collect them

(defn novelty-min
  "Given a db session, returns minimum novelty threshold for reindexing."
  [session]
  (-> session :conn :meta :novelty-min))

;;; =====================================
;;;
;;; Maintenance Utilities
;;;
;;; =====================================


(defn validate-idx-continuity
  "Checks continuity of provided index in that the 'rhs' is equal to the first-flake of the following segment."
  ([conn idx-root] (validate-idx-continuity idx-root false))
  ([conn idx-root throw?] (validate-idx-continuity idx-root throw? nil))
  ([conn idx-root throw? compare]
   (let [node     (async/<!! (dbproto/resolve conn idx-root))
         children (:children node)
         last-i   (dec (count children))]
     (println "Idx children: " (inc last-i))
     (loop [i        0
            last-rhs nil]
       (let [child       (-> children (nth i) val)
             resolved    (async/<!! (dbproto/resolve conn child))
             {:keys [id rhs leftmost?]} child
             child-first (:first child)
             resv-first  (first (:flakes resolved))
             ;; If first-flake is deleted, it should STILL be the first/rhs
             ;; for unresolved nodes to maintain continuity
             continuous? (= last-rhs child-first)]
         #_(println)
         (println "->>" id)
         (println "         first: " child-first)
         (println "    first-resv: " resv-first)
         (println "      last-rhs: " last-rhs)
         (println "     leftmost?: " leftmost?)
         (println "           rhs: " rhs)
         (when (and compare
                    child-first rhs)
           (println "         comp: " (compare child-first rhs)))
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
