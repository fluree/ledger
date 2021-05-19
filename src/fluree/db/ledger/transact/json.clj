(ns fluree.db.ledger.transact.json
  (:require [fluree.db.util.core :as util]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.ledger.transact.txfunction :as txfunction]
            [fluree.db.ledger.transact.identity :as identity]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]))

(defn- txi?
  "Returns true if a transaction item - must be a map and have _id as one of the keys"
  [x]
  (and (map? x) (contains? x "_id")))


(defn resolve-action
  "Returns one of 3 possible action types based one _action and if the
  k-v pairs of the JSON transaction are empty:
  - :add - adding transaction items (which can be over-ridden by a nil value of a key pair)
  - :retract - retracting transaction items (all k-v pairs will attempt deletes)
  - :retract-subject - retract all values for a given subject."
  [_action _id _id-type]
  (if (= :delete (keyword _action))
    (if (= :tempid _id-type)
      (throw (ex-info (str "Deletions with a tempid are not allowed: " _id)
                      {:status 400 :error :db/invalid-transaction}))
      :retract)
    :add))

(defn- resolve-collection-name
  "Resolves collection name from _id"
  [_id {:keys [db-root] :as tx-state}]
  (cond (tempid/TempId? _id)
        (:collection _id)

        (neg-int? _id)
        "_tx"

        (int? _id)
        (->> (flake/sid->cid _id)
             (dbproto/-c-prop db-root :name))))


(defn predicate-details
  "Returns function for predicate to retrieve any predicate details"
  [predicate collection db]
  (if-let [pred-id (or
                     (get-in db [:schema :pred (str collection "/" predicate) :id])
                     (get-in db [:schema :pred predicate :id]))]
    (fn [property] (dbproto/-p-prop db property pred-id))
    (throw (ex-info (str "Predicate does not exist: " predicate)
                    {:status 400 :error :db/invalid-tx}))))

(defn- nested-txi?
  "Returns true if items matches a nested transaction item pattern."
  [pred-info x]
  (and (= :ref (pred-info :type)) (txi? x)))


(defn- update-nested-txi
  "Creates a tempid if needed for a nested txi, and returns
  the txi modified, if applicable.

  The tempid, if created, will be used as the 'o' value of the parent for
  this nested txi."
  [txi tx-state]
  (let [_id  (get txi "_id")
        _id* (if (= :temp-ident (identity/id-type _id))
               (tempid/construct _id tx-state)
               _id)]
    (assoc txi "_id" _id*)))


(defn- base-statement
  "Return the portion of a 'statement' for the subject, which can be used for individual
  predicate+objects to add to."
  [tx-state txi]
  (let [_id        (get txi "_id")
        _action    (get txi "_action")
        _meta      (get txi "_meta")
        _id-type   (identity/id-type _id)
        _id*       (case _id-type
                     :pred-ident
                     (<?? (identity/resolve-ident-strict _id tx-state))

                     :temp-ident
                     (tempid/construct _id tx-state)

                     ;; else
                     _id)
        action     (resolve-action _action _id _id-type)
        collection (resolve-collection-name _id* tx-state)]
    {:iri        nil
     :id         _id*
     :tempid?    (tempid/TempId? _id*)
     :action     action
     :collection collection
     :o-tempid?  nil}))

(declare generate-statement)

(defn- statement-obj
  [base-smt pred-info tx-state idx i obj]
  (let [child (when (nested-txi? pred-info obj)
                (update-nested-txi obj tx-state))
        o     (cond
                child (get child "_id")
                (dbfunctions/tx-fn? obj) (txfunction/->TxFunction obj)
                :else obj)
        idx*  (if i (conj idx i) idx)
        smt   (assoc base-smt :pred-info pred-info
                              :p (pred-info :id)
                              :o o
                              :idx idx*)]
    (if child
      (into [smt] (generate-statement tx-state child idx*))
      [smt])))


(defn- generate-statement
  [{:keys [db-before] :as tx-state} txi idx]
  (let [base-smt  (base-statement tx-state txi)
        p-o-pairs (dissoc txi "_id" "_action" "_meta")]
    (if (and (empty? p-o-pairs) (= :retract (:action base-smt)))
      [(assoc base-smt :action :retract-subject)]           ;; no k-v pairs to iterate over
      (reduce-kv (fn [acc pred obj]
                   (let [pred-info  (predicate-details pred (:collection base-smt) db-before)
                         multi?     (pred-info :multi)
                         idx*       (conj idx pred)
                         statements (if multi?
                                      (->> (if (sequential? obj) (into #{} obj) [obj])
                                           (map-indexed (partial statement-obj base-smt pred-info tx-state idx*))
                                           (apply concat))
                                      (statement-obj base-smt pred-info tx-state idx* nil obj))]
                     (into acc statements)))
                 [] p-o-pairs))))


(defn generate-statements
  [tx-state tx]
  (loop [[txi & r] tx
         i   0
         acc (transient [])]
    (if (nil? txi)
      (persistent! acc)
      (->> (generate-statement tx-state txi [i])
           (reduce conj! acc)
           (recur r (inc i))))))