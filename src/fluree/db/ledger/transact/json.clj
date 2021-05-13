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

(defn- txi-list?
  "Returns true if a sequential? list of txis"
  [x]
  (and (sequential? x)
       (every? txi? x)))

(defn- resolve-nested-txi
  "Takes a predicate-value from a transaction item (what will be the Flakes' .-o value),
  and if that value contains children/nested transaction item(s), returns a two-tuple of
  [tempid(s) nested-txi(s)-with-updated-tempids], else returns nil if no nested txi.

   Tempids need to be validated and generated here, because if they are not unique tempids
   we must make them unique so they point to the correct subjects once flattened."
  [predicate-value tx-state]
  (cond (txi-list? predicate-value)
        (let [txis (map #(assoc % "_id" (tempid/construct (get % "_id") tx-state)) predicate-value)]
          [(map #(get % "_id") txis) txis])

        (txi? predicate-value)
        (let [tempid (tempid/construct (get predicate-value "_id") tx-state)]
          [tempid (assoc predicate-value "_id" tempid)])))

(defn- extract-children*
  "Takes a single transaction item (txi) and returns a two-tuple of
  [updated-txi nested-txi-list] if nested (children) transactions are found.
  If none found, will return [txi nil] where txi will be unaltered."
  [txi tx-state]
  (let [txi+tempid (if (util/temp-ident? (get txi "_id"))
                     (assoc txi "_id" (tempid/construct (get txi "_id") tx-state))
                     txi)]
    (reduce-kv
      (fn [acc k v]
        (cond
          (string? v)
          (if (dbfunctions/tx-fn? v)
            (let [[txi+tempid* found-txis] acc]
              [(assoc txi+tempid* k (txfunction/->TxFunction v)) found-txis])
            acc)

          (or (txi-list? v) (txi? v))
          (let [[nested-ids nested-txis] (resolve-nested-txi v tx-state)
                [txi+tempid* found-txis] acc
                found-txis* (if (sequential? nested-txis)
                              (concat found-txis nested-txis)
                              (conj found-txis nested-txis))]
            [(assoc txi+tempid* k nested-ids) found-txis*])

          :else acc))
      [txi+tempid nil] txi+tempid)))


(defn extract-children
  "From original txi, returns list of all txis included nested ones. If no nested txis are found,
  will just return original txi in a list. When nested txis are found, original txi will be flattened
  by updating the nested txis with their respective tempids. Will recursively check all nested txis for
  children."
  [txi tx-state]
  (let [[updated-txi found-txis] (extract-children* txi tx-state)]
    (if found-txis
      ;; recur on children (nested transactions) for possibly additional children
      (let [found-nested (mapcat #(extract-children % tx-state) found-txis)]
        (conj found-nested updated-txi))
      [updated-txi])))

(defn resolve-action
  "Returns one of 3 possible action types based one _action and if the
  k-v pairs of the JSON transaction are empty:
  - :add - adding transaction items (which can be over-ridden by a nil value of a key pair)
  - :retract - retracting transaction items (all k-v pairs will attempt deletes)
  - :retract-subject - retract all values for a given subject."
  [_action empty-kv? _id _id-type]
  (if (= :delete (keyword _action))
    (do
      (when (= :tempid _id-type)
        (throw (ex-info (str "Deletions with a tempid are not allowed: " _id)
                        {:status 400 :error :db/invalid-transaction})))
      (if empty-kv?
        :retract-subject
        :retract))
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


(defn generate-statements
  [{:keys [db-before] :as tx-state} txi]
  (let [_id            (get txi "_id")
        _action        (get txi "_action")
        _meta          (get txi "_meta")
        _p-o-pairs     (dissoc txi "_id" "_action" "_meta")
        _id-type       (identity/id-type _id)
        _id*           (if (= :pred-ident _id-type)
                         (<?? (identity/resolve-ident-strict _id tx-state))
                         _id)
        action         (resolve-action _action (empty? _p-o-pairs) _id _id-type)
        collection     (resolve-collection-name _id* tx-state)
        base-statement {:iri        nil
                        :id         _id*
                        :tempid?    (= :tempid _id-type)
                        :action     action
                        :collection collection
                        :o-tempid?  nil}]
    (if (= :retract-subject action)
      [base-statement]                                      ;; no k-v pairs to iterate over
      (reduce-kv (fn [acc pred obj]
                   (let [pred-info (predicate-details pred collection db-before)
                         smt       (assoc base-statement :pred-info pred-info
                                                         :p (pred-info :id)
                                                         :o obj)]
                     ;; for multi-cardinality, create a statement for each object
                     (if (pred-info :multi)
                       (reduce #(conj %1 (assoc smt :o %2)) acc (if (sequential? obj) (into #{} obj) [obj]))
                       (conj acc smt))))
                 [] _p-o-pairs))))