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


(defn- generate-statement
  [{:keys [db-before] :as tx-state} txi]
  (let [base-statement (base-statement tx-state txi)
        p-o-pairs      (dissoc txi "_id" "_action" "_meta")]
    (if (and (empty? p-o-pairs) (= :retract (:action base-statement)))
      [(assoc base-statement :action :retract-subject)]     ;; no k-v pairs to iterate over
      (reduce-kv (fn [acc pred obj]
                   (let [pred-info (predicate-details pred (:collection base-statement) db-before)
                         obj*      (if (pred-info :multi)
                                     (if (sequential? obj) (into #{} obj) [obj])
                                     [obj])]
                     (reduce
                       (fn [acc obj]
                         (if (nested-txi? pred-info obj)
                           (let [nested-txi (update-nested-txi obj tx-state)
                                 smt        (assoc base-statement :pred-info pred-info
                                                                  :p (pred-info :id)
                                                                  :o (get nested-txi "_id"))
                                 smts       (into [smt]
                                                  (generate-statement tx-state nested-txi))]
                             (into acc smts))
                           (conj acc (assoc base-statement :pred-info pred-info
                                                           :p (pred-info :id)
                                                           :o (if (dbfunctions/tx-fn? obj)
                                                                (txfunction/->TxFunction obj)
                                                                obj)))))
                       acc obj*)))
                 [] p-o-pairs))))


(defn generate-statements
  [tx-state tx]
  (->> tx
       (pmap (partial generate-statement tx-state))
       (apply concat)))