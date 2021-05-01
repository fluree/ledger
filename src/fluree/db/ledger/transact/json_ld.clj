(ns fluree.db.ledger.transact.json-ld
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.transact.identity :as identity]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.util.iri :as iri-util])
  (:import (fluree.db.flake Flake)))


(defn resolve-prefix
  "Returns two-tuple of [iri prefix].

  If prefix detected, but could not resolve full iri, returns: [nil prefix]

  If no prefix detected, returns [iri nil]"
  [db compact-iri]
  (if-let [[prefix rest] (iri-util/parse-prefix compact-iri)]
    (if-let [iri (some-> (<?? (query-range/index-range db :spot = [["_prefix/prefix" prefix] const/$_prefix:iri]))
                         ^Flake first
                         .-o)]
      [(str iri rest) prefix]
      [nil prefix])
    [compact-iri nil]))


(defn system-collections
  "Returns map of defined base-iris -> collection."
  [db]
  (->> db
       :schema
       :coll
       (filter #(number? (key %)))
       vals
       (filter :base-iri)
       (reduce #(assoc %1 (:base-iri %2) (:name %2)) {})))


(defn normalize-txi
  "Expands transaction item from compacted iris to full iri."
  [txi context]
  (reduce-kv (fn [acc k v]
               (assoc acc (iri-util/expand k context) (iri-util/expand v context)))
             {} txi))


(defn collector-fn
  "Function that given an iri, return the collection that is supposed to be used."
  [sys-collections]
  (let [match-iris (->> sys-collections
                        keys
                        (sort-by #(* -1 (count %))))        ;; want longest iris checked first
        match-fns  (mapv
                     (fn [base-iri]
                       (let [re         (re-pattern (str "^" base-iri))
                             collection (get sys-collections base-iri)]
                         (fn [iri]
                           (when (re-find re iri)
                             collection))))
                     match-iris)]
    (fn [iri]
      (or (some (fn [match-fn] (match-fn iri)) match-fns)
          (throw (ex-info (str "The iri does not match any collections, and no default collection is specified: "
                               iri
                               " Either specify _collection/baseIRI for a collection that will match, or set a "
                               "collection as a default by setting _collection/baseIRI to '' (empty string).")
                          {:status 400
                           :error  :db/invalid-transaction}))))))

(defn build-collector-fn
  [db]
  (let [sys-collections (system-collections db)]
    (collector-fn sys-collections)))


(defn compact-txi
  "With a system context, compacts txi iris to use prefixes defined inside
  of the _prefix collection."
  [txi prefix-resolver]
  (reduce-kv (fn [acc k v]
               (assoc acc (or (prefix-resolver k) k)
                          (or (prefix-resolver v) v)))
             {} txi))


(defn system-predicate?
  "Returns true if a predicate is a special/reserved item.
  This includes anything starting with '@' (JSON-LD), or starting with '_' (Fluree)."
  [s]
  (case (first s)
    \@ true
    \_ true
    false))


;; TODO - resolve-action is duplicated with json namespace - look to consolidate somewhere.
(defn resolve-action
  "Returns one of 3 possible action types based one _action and if the
  k-v pairs of the JSON transaction are empty:
  - :add - adding transaction items (which can be over-ridden by a nil value of a key pair)
  - :retract - retracting transaction items (all k-v pairs will attempt deletes)
  - :retract-subject - retract all values for a given subject."
  [_action empty-kv?]
  (if (= :delete (keyword _action))
    (if empty-kv?
      :retract-subject
      :retract)
    :add))

(defn predicate-details
  "Returns function for predicate to retrieve any predicate details"
  [predicate collection db]
  (if-let [pred-id (or
                     (get-in db [:schema :pred (str collection "/" predicate) :id])
                     (get-in db [:schema :pred predicate :id]))]
    (fn [property] (dbproto/-p-prop db property pred-id))
    (throw (throw (ex-info (str "Predicate does not exist: " predicate)
                           {:status 400 :error :db/invalid-tx})))))


(defn generate-statement
  [{:keys [db-before tx-context collector] :as tx-state} txi]
  (log/warn "Generate statement: " txi)
  (let [local-context  (if-let [txi-context (get txi "@context")]
                         (iri-util/expanded-context txi-context tx-context)
                         tx-context)
        _              (log/warn "generate-statement - local-context: " local-context)
        iri            (get txi "@id")
        expanded-iri   (iri-util/expand iri local-context)           ;; first expand iri with local context
        _              (log/warn "generate-statement - expanded-iri: " expanded-iri)
        collection     (collector expanded-iri)
        _              (log/warn "generate-statement - collection: " collection)
        _action        (get txi "@action")
        _meta          (get txi "@meta")
        _p-o-pairs     (dissoc txi "@id" "@context" "@action")
        action         (resolve-action _action (empty? _p-o-pairs))
        _              (log/warn "generate-statement - action: " action)
        id             (<?? (identity/resolve-iri expanded-iri tx-state))
        _              (log/warn "generate-statement - id: " id)
        base-statement {:iri        iri
                        :id         id
                        :tempid?    (tempid/TempId? id)
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


(defn tx?
  "Returns true if the transaction supplied looks like a JSON-LD version."
  [tx]
  (boolean
    (or
      (and (get tx "@context") (get tx "@graph"))
      (get-in tx [0 "@id"]))))


(defn get-tx-context
  "Returns the context to be used for the transaction.
  If there is a @context defined for the tx, merges it into the db's context,
  else returns the db's context."
  [db tx]
  (if-let [tx-ctx (get tx "@context")]
    (iri-util/expanded-context tx-ctx (-> db :schema :prefix))
    (-> db :schema :prefix)))


(defn generate-statements
  [{:keys [db-before tx-context collector] :as tx-state} tx]
  ;; TODO - if we maintain tx-context here, delete from tx-state
  (let [tx-data (or (get tx "@graph")
                    (when (sequential? tx) tx)
                    (throw (ex-info (str "Invalid transaction.") {:status 400 :error :db/invalid-transaction})))]
    (mapcat #(generate-statement tx-state %) tx-data)))




;; TODO
;; - add prefix-resolver to tx-state


(comment
  (def db (<?? (fdb/db (:conn user/system) "prefix/d")))

  (def sys-coll (system-collections db))
  sys-coll


  (def prefix-resolver (iri-util/compact-fn sys-context))
  (prefix-resolver "http://www.w3.org/2000/01/rdf-schema#subClass")

  (def context (iri-util/expanded-context {"nc"        "http://release.niem.gov/niem/niem-core/4.0/#",
                                           "j"         "http://release.niem.gov/niem/domains/jxdm/6.0/#",
                                           "age"       "nc:PersonAgeMeasure",
                                           "value"     "nc:MeasureIntegerValue",
                                           "units"     "nc:TimeUnitCode",
                                           "hairColor" "j:PersonHairColorCode",
                                           "name"      "nc:PersonName",
                                           "given"     "nc:PersonGivenName",
                                           "surname"   "nc:PersonSurName",
                                           "suffix"    "nc:PersonNameSuffixText",
                                           "nickname"  "nc:PersonPreferredName",}
                                          #_sys-context))

  context
  (def ctx-compactor (iri-util/compact-fn context))

  (ctx-compactor "http://release.niem.gov/niem/niem-core/4.0/#2PersonName")


  (resolve-prefix db "fluree:Brian")

  )
