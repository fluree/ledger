(ns fluree.db.ledger.indexing
  (:require [clojure.data.avl :as avl]
            [clojure.tools.logging :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.storage.core :as storage]
            [fluree.db.session :as session]
            [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto])
  (:import (fluree.db.flake Flake)
           (java.time Instant)))


;;; run an indexing processing a database

(def ^:dynamic *overflow-bytes* 500000)
(def ^:dynamic *underflow-bytes* 50000)

;; an index is dirty if there is any novelty associated with it


(defn overflow?
  [size-bytes]
  (> size-bytes *overflow-bytes*))


(defn underflow?
  [size-bytes]
  (< size-bytes *underflow-bytes*))


(defn split-flakes
  "Splits into n parts. "
  [flakes size]
  (if (< size *overflow-bytes*)                             ;; shouldn't be called if this is the case, log warning and return unaffected
    (do
      (log/warn "Split flakes called on flakes that are < *overflow-kb*" {:size size :overflow-kb *overflow-bytes*})
      [flakes])
    (let [bytes-min (/ *overflow-bytes* 2)
          splits-n  (Math/round (float (/ size bytes-min)))
          ;; need to ensure no down-rounding else could have an inadvertent extra segment
          seg-bytes (/ size splits-n)]
      (loop [[f & r] flakes
             curr      []
             curr-size 0
             segments  []]
        (let [f-size     (flake/size-flake f)
              curr-size* (+ curr-size f-size)
              curr*      (conj curr f)
              full?      (> curr-size* seg-bytes)
              segments*  (if full?
                           (conj segments {:flakes curr* :size curr-size*})
                           segments)]
          (if r
            (if full?
              (recur r [] 0 segments*)
              (recur r curr* curr-size* segments*))
            (if full?                                       ;; ensure if full? not exact, last segment gets added in properly
              segments*
              (conj segments {:flakes curr* :size curr-size*}))))))))


(defn get-history-in-range
  "Filters through history and pulls only items within the provided range.
  >= lhs, < rhs. When rhs is nil, no end."
  [history lhs rhs compare]
  (let [pred (if (nil? rhs)
               (fn [^Flake f] (>= 0 (compare lhs f)))
               (fn [^Flake f] (and (>= 0 (compare lhs f))
                                   (< 0 (compare rhs f)))))]
    (filter pred history)))

(defn find-combine-leaf
  "Returns n combine-leaf combine-bytes combine-his"
  [all-leaves leaf-i t idx-novelty idx-type direction remove-preds]
  (go-try
    (let [skip-fn (if (= :next direction) - +)
          end?    (if (= :next direction)
                    (fn [n]
                      (= 0 (- leaf-i n)))
                    (let [last-leaf (-> all-leaves count dec)]
                      (fn [n]
                        (= last-leaf (+ leaf-i n)))))]
      (loop [n   1
             his []]
        (let [combine-node     (-> (nth all-leaves (skip-fn leaf-i n)) val)
              resolved         (<? (dbproto/-resolve-to-t combine-node t idx-novelty false remove-preds))
              history          (<? (dbproto/-resolve-history-range combine-node nil t idx-novelty))
              comparator       (.comparator (:flakes resolved))
              combine-leaf-his (get-history-in-range history (:first combine-node) (:rhs combine-node) comparator)
              his*             (into his combine-leaf-his)]
          (cond (not (empty? (:flakes resolved)))
                [n resolved (flake/size-bytes (:flakes resolved)) his*]

                (end? n)
                [n resolved (flake/size-bytes (:flakes resolved)) his*]

                :else
                (recur (inc n) his*)))))))


(defn index-leaf
  "Given a node, idx-novelty, returns [ [nodes] skip n].
  Skip is either: nil, :next, or :previous.
  N is how many to skip, could be a situation where multiple empty nodes
  "
  ([conn network dbid node block t idx-novelty rhs all-leaves leaf-i]
   (index-leaf conn network dbid node block t idx-novelty rhs all-leaves leaf-i #{}))
  ([conn network dbid node block t idx-novelty rhs all-leaves leaf-i remove-preds]
   (go-try
     (let [resolved-ch (dbproto/-resolve-to-t node t idx-novelty false remove-preds) ;; pull history and node in parallel
           history-ch  (dbproto/-resolve-history-range node nil t idx-novelty)
           {:keys [config leftmost?]} node
           fflake      (:first node)
           idx-type    (:index-type config)
           resolved    (<? resolved-ch)
           node-bytes  (flake/size-bytes (:flakes resolved))
           overflow?   (overflow? node-bytes)
           underflow?  (and (underflow? node-bytes) (not= 1 (count all-leaves)))
           history     (<? history-ch)]
       (cond
         overflow?
         (let [splits     (split-flakes (:flakes resolved) node-bytes)
               comparator (.comparator (:flakes resolved))]
           (loop [split-i (dec (count splits))
                  rhs'    rhs
                  acc     (list)]
             (let [{:keys [flakes size]} (nth splits split-i)
                   first-flake     (if (zero? split-i)
                                     fflake                 ;; don't change the node's existing first-flake, even if flake no longer exists to keep :rhs of left index segment consistent
                                     (first flakes))
                   base-id         (str (util/random-uuid))
                   his-split       (get-history-in-range history first-flake rhs' comparator)
                   id              (<? (storage/write-leaf conn network dbid idx-type base-id
                                                           flakes his-split))
                   child-leftmost? (and leftmost? (zero? split-i))
                   child-node      (storage/map->UnresolvedNode
                                     {:conn      conn :config config
                                      :dbid      dbid :id id :leaf true
                                      :first     first-flake :rhs rhs'
                                      :size      size :block block :t t
                                      :leftmost? child-leftmost?})
                   acc*            (conj acc child-node)]
               (if (zero? split-i)
                 [acc* nil nil]
                 (recur (dec split-i) first-flake acc*)))))

         underflow?
         ;;; First determine skip direction
         (let [[skip n combine-leaf combine-bytes
                combine-his] (cond leftmost?
                                   (let [[n combine-leaf combine-bytes combine-his]
                                         (<? (find-combine-leaf all-leaves leaf-i t idx-novelty idx-type :previous remove-preds))]
                                     [:previous n combine-leaf combine-bytes combine-his])

                                   ;; rightmost
                                   (= leaf-i (-> all-leaves count dec))
                                   (let [[n combine-leaf combine-bytes combine-his]
                                         (<? (find-combine-leaf all-leaves leaf-i t idx-novelty idx-type :next remove-preds))]
                                     [:next n combine-leaf combine-bytes combine-his])

                                   ;; in the middle
                                   :else (let [;; prev-leaf could be empty
                                               [prev-n prev-leaf prev-bytes prev-combine-his]
                                               (<? (find-combine-leaf all-leaves leaf-i t idx-novelty idx-type :previous remove-preds))
                                               ;; next-leaf could be empty
                                               [next-n next-leaf next-bytes next-combine-his]
                                               (<? (find-combine-leaf all-leaves leaf-i t idx-novelty idx-type :next remove-preds))]
                                           (if (> prev-bytes next-bytes)
                                             [:next next-n next-leaf next-bytes next-combine-his]
                                             [:previous prev-n prev-leaf prev-bytes prev-combine-his])))
               base-id          (str (util/random-uuid))
               comparator       (.comparator (:flakes resolved))
               current-node-his (get-history-in-range history fflake rhs comparator)
               his-in-range     (into current-node-his combine-his)
               flakes           (set (concat (:flakes resolved) (:flakes combine-leaf)))
               id               (<? (storage/write-leaf conn network dbid idx-type base-id
                                                        flakes his-in-range))
               size             (+ node-bytes combine-bytes)
               ;; current node might be empty, so we need to get first and rhs from node, NOT resolved
               [first-flake rhs] (if (= skip :previous)
                                   [(:first node) (:rhs combine-leaf)]
                                   ;; if it's next, combine leaf :first could be nil if we hit the leftmost
                                   [(or (:first combine-leaf) (-> (nth all-leaves (- leaf-i n)) val :first))
                                    (:rhs node)])
               leftmost?        (or leftmost?
                                    (when (= :next skip) (= 0 (- leaf-i n)))
                                    false)
               child-node       (storage/map->UnresolvedNode
                                  {:conn  conn :config config
                                   :dbid  dbid :id id :leaf true
                                   :first first-flake :rhs rhs
                                   :size  size :block block
                                   :t     t :leftmost? leftmost?})]
           [[child-node] skip n])

         :else
         (let [base-id    (str (util/random-uuid))
               flakes     (:flakes resolved)
               id         (<? (storage/write-leaf conn network dbid idx-type base-id
                                                  flakes history))
               child-node (storage/map->UnresolvedNode
                            {:conn      conn :config config
                             :dbid      dbid :id id :leaf true
                             :first     fflake :rhs rhs
                             :size      node-bytes :block block :t t
                             :leftmost? leftmost?})]
           [[child-node] nil nil]))))))


(defn novelty-subrange
  [novelty first-flake rhs leftmost?]
  (try
    (cond
      (and leftmost? rhs) (avl/subrange novelty < rhs)
      rhs (avl/subrange novelty >= first-flake < rhs)
      leftmost? novelty                                     ;; at left and no rhs... all novelty applies
      :else (avl/subrange novelty >= first-flake))
    (catch Exception e
      (log/error (str "Error indexing. Novelty subrange error: " (.getMessage e))
                 (pr-str {:first-flake first-flake :rhs rhs :leftmost? leftmost? :novelty novelty}))
      (throw e))))


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

(comment

  (def myset #{1 2 3 10 9 4})

  (def rhs 10)
  (def frst 8)

  (reduce #(if (and (>= %2 frst) (if rhs (<= %2 rhs) true))
             (conj %1 %2)
             %1) #{} myset)

  )

(defn index-branch
  "Gets called when a root index is dirty, and for all sub-roots."
  ([conn network dbid node idx-novelty block t rhs progress]
   (index-branch conn network dbid node idx-novelty block t rhs progress #{}))
  ([conn network dbid node idx-novelty block t rhs progress remove-preds]
   (go-try
     (let [resolved   (<? (dbproto/-resolve node))
           base-id    (str (util/random-uuid))
           {:keys [config children]} resolved
           idx-type   (:index-type config)
           child-n    (count children)
           at-leaf?   (:leaf (val (first children)))
           children*  (loop [child-i (dec child-n)
                             rhs     rhs
                             acc     (empty children)]
                        (if (< child-i 0)                   ;; at end of result set
                          acc
                          (let [child         (val (nth children child-i))
                                child-rhs     (:rhs child)
                                _             (when-not (or (= (dec child-i) child-n) (= child-rhs rhs))
                                                (throw (ex-info (str "Something went wrong. Child-rhs does not equal rhs: " {:child-rhs child-rhs :rhs rhs})
                                                                {:status 500
                                                                 :error  :db/unexpected-error})))
                                child-first   (:first child)
                                novelty       (novelty-subrange idx-novelty child-first child-rhs (:leftmost? child))
                                remove-preds? (if remove-preds
                                                ;remove-preds
                                                (let [child-first-pred (.-p child-first)
                                                      child-rhs-pred   (when child-rhs (.-p child-rhs))]
                                                  (reduce #(if (and (>= %2 child-first-pred)
                                                                    (if child-rhs-pred
                                                                      (<= %2 child-rhs-pred) true))
                                                             (conj %1 %2) %1) #{} remove-preds))
                                                #{})
                                dirty?        (or (not (empty? novelty)) (not (empty? remove-preds?)))
                                [new-nodes skip n] (if dirty?
                                                     (if at-leaf?
                                                       (<? (index-leaf conn network dbid child block t idx-novelty child-rhs children child-i remove-preds?))
                                                       [(<? (index-branch conn network dbid child idx-novelty block t child-rhs progress remove-preds?)) nil nil])
                                                     [[child] nil nil])
                                new-rhs       (:first (first new-nodes))
                                ;; In order for this to work, the FIRST node has to be the leftmost.
                                next-i        (if skip
                                                (let [skip-next? (= skip :next)
                                                      skip-fn    (if skip-next? - +)
                                                      next-i*    (if skip-next?
                                                                   (- child-i (inc n))
                                                                   (dec child-i))
                                                      garbage    (map #(skip-fn child-i %) (range 1 (inc n)))
                                                      _          (loop [[rm & r] garbage]
                                                                   (when rm
                                                                     (let [id (-> (nth children rm) val :id)]
                                                                       (add-garbage id at-leaf? progress))
                                                                     (recur r)))] next-i*)


                                                (dec child-i))
                                acc*          (reduce #(assoc %1 (:first %2) %2) acc new-nodes)]
                            ;; add dirty node indexes to garbage
                            (when dirty?
                              (add-garbage (:id child) at-leaf? progress))
                            (recur next-i new-rhs acc*))))
           node-bytes (-> (keys children*)
                          (flake/size-bytes))
           id         (<? (storage/write-branch conn network dbid idx-type base-id children*))
           new-node   (storage/map->UnresolvedNode
                        {:conn      conn :config config
                         :network   network :dbid dbid
                         :id        id :leaf false
                         :first     (key (first children*)) :rhs rhs
                         :size      node-bytes :block block :t t
                         :leftmost? (:leftmost? node)})]
       new-node))))


(defn index-root
  "Indexes an index-type root (:spot, :psot, :post, or :opst).

  Progress atom tracks progress and retains list of garbage indexes."
  ([db progress-atom idx-type]
   (index-root db progress-atom idx-type #{}))
  ([db progress-atom idx-type remove-preds]
   (go-try
     (assert (#{:spot :psot :post :opst} idx-type) (str "Reindex attempt on unknown index type: " idx-type))
     (let [{:keys [conn novelty block t network dbid]} db
           idx-novelty (get novelty idx-type)
           dirty?      (or (not (empty? idx-novelty)) remove-preds)
           idx-root    (get db idx-type)]
       (if-not dirty?
         idx-root
         (do
           ;; add main index node key to garbage for collection
           (add-garbage (:id idx-root) false progress-atom)
           (<? (index-branch conn network dbid idx-root idx-novelty block t nil progress-atom remove-preds))))))))

;; TODO - should track new index segments and if failure, garbage collect them


(defn index
  "Write each index type, writes happen from right to left in the tree
  so we know the 'rhs' value of each node going into it."
  ([db]
   (index db {:status "ready"}))
  ([db {:keys [status message ecount remove-preds]}]
   (go-try
     (let [{:keys [novelty block t network dbid]} db
           db-dirty?    (or (some #(not-empty (get novelty %)) [:spot :psot :post :opst])
                            remove-preds)
           novelty-size (:size novelty)
           progress     (atom {:garbage   []                ;; hold keys of old index segments we can garbage collect
                               :size      novelty-size
                               :completed 0})
           start-time   (Instant/now)]
       (log/info (str "Index Update begin at: " start-time) {:network      network
                                                             :dbid         dbid
                                                             :t            t
                                                             :block        block
                                                             :novelty-size novelty-size})
       (if-not db-dirty?
         db
         (let [spot-ch    (index-root db progress :spot)    ;; indexes run in parallel
               psot-ch    (index-root db progress :psot)
               post-ch    (index-root db progress :post remove-preds)
               opst-ch    (index-root db progress :opst)
               indexed-db (-> db
                              (assoc :spot (<? spot-ch)
                                     :psot (<? psot-ch)
                                     :post (<? post-ch)
                                     :opst (<? opst-ch))
                              (update-in [:novelty :spot] empty) ;; retain sort order of indexes
                              (update-in [:novelty :psot] empty)
                              (update-in [:novelty :post] empty)
                              (update-in [:novelty :opst] empty)
                              (assoc-in [:novelty :size] 0)
                              (assoc-in [:stats :indexed] block))]
           ;; wait until confirmed writes before returning
           ;; TODO - ideally issue garbage/root writes to RAFT together as a tx, currently requires waiting for both through raft sync
           (<? (storage/write-garbage indexed-db @progress))
           (<? (storage/write-db-root indexed-db ecount))
           (log/info (str "Index Update end at: " (Instant/now)) {:network      network
                                                                  :dbid         dbid
                                                                  :block        block
                                                                  :t            t
                                                                  :idx-duration (- (.toEpochMilli (Instant/now))
                                                                                   (.toEpochMilli start-time))})
           indexed-db))))))


(defn novelty-min
  "Given a db session, returns minimum novelty threshold for reindexing."
  [session]
  (-> session :conn :meta :novelty-min))


(defn index*
  ([session {:keys [remove-preds] :as opts}]
   (go-try
     (if (session/indexing? session)
       false
       (let [latest-db     (<? (session/current-db session))
             novelty-size  (get-in latest-db [:novelty :size])
             novelty-min   (novelty-min session)
             remove-preds? (and (not (nil? remove-preds)) (not (empty? remove-preds)))
             needs-index?  (or remove-preds? (>= novelty-size novelty-min))]
         (if needs-index?
           ;; kick off indexing with this DB
           (<? (index* session latest-db opts))
           ;; no index needed, return false
           false)))))
  ([session db opts]
   (go-try
     (let [{:keys [conn block network dbid]} db
           last-index (session/indexed session)]
       (cond
         (and last-index (<= block last-index))
         (do
           (log/info "Index called on DB but last index isn't older."
                     {:last-index last-index :block block :db (pr-str db) :session (pr-str session)})
           false)

         (session/acquire-indexing-lock! session block)
         (let [updated-db (<? (index db opts))
               group      (-> updated-db :conn :group)]
           ;; write out index point
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
  "Checks continuity of provided index in that the 'rhs' is equal to the first-flake of the following segment."
  ([idx-root] (validate-idx-continuity idx-root false))
  ([idx-root throw?] (validate-idx-continuity idx-root throw? nil))
  ([idx-root throw? compare]
   (let [node     (async/<!! (dbproto/-resolve idx-root))
         children (:children node)
         last-i   (dec (count children))]
     (println "Idx children: " (inc last-i))
     (loop [i        0
            last-rhs nil]
       (let [child       (-> children (nth i) val)
             resolved    (async/<!! (dbproto/-resolve child))
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
    [db]
    (let [spot-comp (.comparator (-> db :novelty :spot))
          post-comp (.comparator (-> db :novelty :post))
          psot-comp (.comparator (-> db :novelty :psot))
          opst-comp (.comparator (-> db :novelty :opst))]
      (do (validate-idx-continuity (:spot db) true spot-comp)
          (validate-idx-continuity (:post db) true post-comp)
          (validate-idx-continuity (:psot db) true psot-comp)
          (validate-idx-continuity (:opst db) true opst-comp))))

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

  (validate-idx-continuity (:spot db) true spot-comp)

  (time (async/<!! (fluree.db.ledger.snapshot/create-snapshot conn "test" "one")))

  (async/<!! (fluree.db.api/new-ledger-async
               conn "test/affssfsfsadfas"
               {:snapshot "test/one/snapshot/1587130669908.avro"}))


  (time (async/<!! (fluree.db.ledger.snapshot/create-snapshot-no-history conn "smol" "one")))

  )