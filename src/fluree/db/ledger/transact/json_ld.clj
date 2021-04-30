(ns fluree.db.ledger.transact.json-ld
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.json :as json]
            [fluree.db.constants :as const]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.transact.identity :as identity]
            [fluree.db.ledger.transact.tempid :as tempid])
  (:import (fluree.db.flake Flake)))


(defn parse-prefix
  [s]
  (let [[_ prefix rest] (re-find #"([^:]+):(.+)" s)]
    (if (nil? prefix)
      nil
      [prefix rest])))


(defn resolve-prefix
  "Returns two-tuple of [iri prefix].

  If prefix detected, but could not resolve full iri, returns: [nil prefix]

  If no prefix detected, returns [iri nil]"
  [db compact-iri]
  (if-let [[prefix rest] (parse-prefix compact-iri)]
    (if-let [iri (some-> (<?? (query-range/index-range db :spot = [["_prefix/prefix" prefix] const/$_prefix:iri]))
                         ^Flake first
                         .-o)]
      [(str iri rest) prefix]
      [nil prefix])
    [compact-iri nil]))


(defn system-context
  "Returns map of system context."
  [db]
  (let [prefix-cid    (dbproto/-c-prop db :partition "_prefix")
        prefix-flakes (<?? (query-range/index-range db :spot
                                                    >= [(flake/max-subject-id prefix-cid)]
                                                    <= [(flake/min-subject-id prefix-cid)]))]
    (->> prefix-flakes
         (group-by #(.-s ^Flake %))
         (reduce (fn [acc [_ p-flakes]]
                   (let [prefix (some #(when (= const/$_prefix:prefix (.-p %))
                                         (.-o %)) p-flakes)
                         iri    (some #(when (= const/$_prefix:iri (.-p %))
                                         (.-o %)) p-flakes)]
                     (if (and prefix iri)
                       (assoc acc prefix iri)
                       acc))) {}))))

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


(defn expanded-context
  "Returns a fully expanded context map from a source map"
  ([context] (expanded-context context nil))
  ([context default-context]
   (merge default-context
          (->> context
               (reduce-kv
                 (fn [acc prefix iri]
                   (if-let [[val-prefix rest] (parse-prefix iri)]
                     (if-let [iri-prefix (or (get acc val-prefix)
                                             (get default-context val-prefix))]
                       (assoc acc prefix (str iri-prefix rest))
                       acc)
                     acc))
                 context)))))

(defn expand
  "Expands a compacted iri string to full iri.

  If the iri is not compacted, returns original iri string."
  [compact-iri context]
  (if-let [[prefix rest] (parse-prefix compact-iri)]
    (if-let [p-iri (get context prefix)]
      (str p-iri rest)
      compact-iri)
    compact-iri))

(defn normalize-txi
  "Expands transaction item from compacted iris to full iri."
  [txi context]
  (reduce-kv (fn [acc k v]
               (assoc acc (expand k context) (expand v context)))
             {} txi))


(defn reverse-context
  "Flips context map from prefix -> iri, to iri -> prefix"
  [context]
  (reduce-kv #(assoc %1 %3 %2) {} context))

(defn compact-fn
  "Returns a single prefix-resolving function based on the system context.

  If a prefix can be resolved, returns a 3-tuple of:
  [compacted-iri prefix base-iri]"
  [sys-context]
  (let [flipped    (reverse-context sys-context)            ;; flips context map
        match-iris (->> flipped
                        keys
                        (sort-by #(* -1 (count %))))        ;; want longest iris checked first
        match-fns  (mapv
                     (fn [base-iri]
                       (let [count  (count base-iri)
                             re     (re-pattern (str "^" base-iri))
                             prefix (get flipped base-iri)]
                         (fn [iri]
                           (when (re-find re iri)
                             [(str prefix ":" (subs iri count)) prefix base-iri]))))
                     match-iris)]
    (fn [iri]
      (some (fn [match-fn]
              (match-fn iri))
            match-fns))))


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
                         (merge tx-context (expanded-context txi-context))
                         tx-context)
        _              (log/warn "generate-statement - local-context: " local-context)
        iri            (get txi "@id")
        expanded-iri   (expand iri local-context)           ;; first expand iri with local context
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
  "When a context is provided as part of a transaction, expands
  it so it can be carried in tx-state."
  [tx]
  (when-let [ctx (get tx "@context")]
    (expanded-context ctx)))


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

  (def sys-ctx (system-context db))
  sys-ctx
  (def sys-coll (system-collections db))
  sys-coll


  (def prefix-resolver (compact-fn sys-context))
  (prefix-resolver "http://www.w3.org/2000/01/rdf-schema#subClass")

  (def context (expanded-context {"nc"        "http://release.niem.gov/niem/niem-core/4.0/#",
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
  (def ctx-compactor (compact-fn context))

  (ctx-compactor "http://release.niem.gov/niem/niem-core/4.0/#2PersonName")


  (resolve-prefix db "fluree:Brian")

  )
