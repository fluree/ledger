(ns fluree.db.ledger.transact.json
  (:require [fluree.db.ledger.transact.subject :refer :all]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [clojure.core.async :as async]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.json :as json]
            [fluree.db.spec :as fspec]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.util.tx :as tx-util]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.query.schema :as schema]
            [fluree.db.ledger.transact.retract :as tx-retract]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.ledger.transact.tags :as tags]
            [fluree.db.ledger.transact.txfunction :as txfunction]
            [fluree.db.ledger.transact.auth :as tx-auth]
            [fluree.db.ledger.transact.tx-meta :as tx-meta])
  (:import (fluree.db.flake Flake)))

;; TODO - add ^:const
(def parallelism 10)

(defn register-validate-fn
  [f async? {:keys [validate-fn] :as tx-state}]
  (swap! validate-fn (fn [f-map]
                       (if async?
                         (update f-map :async conj f)
                         (update f-map :sync conj f)))))

(defn- txi?
  "Returns true if a transaction item - must be a map and have _id as one of the keys"
  [x]
  (and (map? x) (contains? x :_id)))

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
        (let [txis (map #(assoc % :_id (tempid/new (:_id %) tx-state)) predicate-value)]
          [(map :_id txis) txis])

        (txi? predicate-value)
        (let [tempid (tempid/new (:_id predicate-value) tx-state)]
          [tempid (assoc predicate-value :_id tempid)])

        :else nil))


(defn resolve-ident-strict
  "Resolves ident (from cache if exists). Will throw exception if ident cannot be resolved."
  [ident {:keys [db-root idents] :as tx-state}]
  (go-try
    (if-let [cached (get @idents ident)]
      cached
      (let [resolved (<? (dbproto/-subid db-root ident false))]
        (if (nil? resolved)
          (throw (ex-info (str "Invalid identity, does not exist: " (pr-str ident))
                          {:status 400 :error :db/invalid-tx}))
          (do
            (swap! idents assoc ident resolved)
            resolved))))))


(defn- resolve-collection-name
  "Resolves collection name from _id"
  [_id tx-state]
  (cond (tempid/TempId? _id)
        (:collection _id)

        (neg-int? _id)
        "_tx"

        (int? _id)
        (->> (flake/sid->cid _id)
             (dbproto/-c-prop (:db tx-state) :name))))

(defn- resolve-collection-id
  "Resolves collection id from collection name"
  [db _collection]
  (dbproto/-c-prop db :id _collection))

(defn resolve-action
  "Returns top level action as keyword for subject based on user-supplied action (if available),
  if the subject was empty (no predicate-object pairs provided), or if the subject is using a tempid"
  [action empty-subject? tempid?]
  (let [raise (fn [action]
                (ex-info (str "Invalid _action: " (pr-str action)) {}))]
    (cond
      (string? action) (or (-> (keyword action)
                               #{:delete :insert :update})
                           (throw (raise action)))
      (nil? action) (cond tempid? :insert
                          empty-subject? :delete
                          :else :update)
      :else (throw (raise action)))))


(defn predicate-details
  "Returns function for predicate to retrieve any predicate details"
  [predicate collection db]
  (let [full-name (if-let [a-ns (namespace predicate)]
                    (str a-ns "/" (name predicate))
                    (str collection "/" (name predicate)))]
    (if-let [pred-id (get-in db [:schema :pred full-name :id])]
      (fn [property] (dbproto/-p-prop db property pred-id))
      (throw (throw (ex-info (str "Predicate does not exist: " predicate)
                             {:status 400 :error :db/invalid-tx}))))))

(defn conform-object-value
  "Attempts to coerce any value to internal form."
  [object type]
  ;; note type :ref and :tag are not sent to this function, so
  (case type
    :string (if (string? object)
              object
              (fspec/type-check object type))
    :json (try (json/stringify object)
               (catch Exception _
                 (throw (ex-info (str "Unable to serialize JSON from value: " (pr-str object))
                                 {:status 400
                                  :error  :db/invalid-tx}))))

    :ref object                                             ;; will have already been conformed
    :tag object                                             ;; will have already been conformed
    ;; else
    ;; TODO - type-check creates an atom to hold errors, we really just need to throw exception if error exists
    (fspec/type-check object type)
    ))

(defn register-unique
  "Registers unique value in the tx-state and return true if successful.
  Will be unsuccessful if the ident already exists in the unique value cache
  and return false.

  Ident is a two-tuple of [pred-id object/value]"
  [ident {:keys [uniques] :as tx-state}]
  (if (contains? @uniques ident)
    false                                                   ;; already registered, should throw downstream
    (do (swap! uniques conj ident)
        true)))

(defn resolve-unique
  "If predicate is unique, need to determine if any matching values already exist both
  in existing ledger, but also within the transaction.

  If they exist in the existing ledger, but upsert? is true, we can resolve the
  tempid to subject id (but only if a different upsert? predicate didn't already
  resolve it to a different subject!)

  If error is not thrown, returns the provided object argument."
  [object-ch _id pred-info {:keys [db t] :as tx-state}]
  (go-try
    (let [object       (<? object-ch)
          pred-id      (pred-info :id)
          ;; create a two-tuple of [object-value async-chan-with-found-subjects]
          ;; to iterate over. Kicks off queries (if multi) in parallel.
          obj+subj-chs (->> (if (pred-info :multi) object [object])
                            (map #(vector % (query-range/index-range db :post = [pred-id %]))))]
      ;; use a loop here so we can use async to full extent
      (loop [[[obj subject-ch] & r] obj+subj-chs]
        (if-not obj
          ;; finished, return original object - no errors
          object
          ;; check out next ident
          (let [existing-flake ^Flake (first (<? subject-ch))]
            (when (false? (register-unique [pred-id obj] tx-state))
              (throw (ex-info (str "Unique predicate " (pred-info :name) " was used more than once "
                                   "in the transaction with the value of: " object ".")
                              {:status 400 :error :db/invalid-tx})))
            (cond
              ;; no matching existing flake, move on
              (nil? existing-flake) (recur r)

              ;; lookup subject matches subject, will end up ignoring insert downstream unless :retractDuplicates is true
              (= (.-s existing-flake) _id) (recur r)

              ;; found existing subject and tempid, so set tempid value (or throw if already set to different subject)
              (and (tempid/TempId? _id) (pred-info :upsert))
              (do
                (tempid/set _id (.-s existing-flake) tx-state) ;; will throw if tempid was already set to a different subject
                (recur r))

              ;; tempid, but not upsert - throw
              (tempid/TempId? _id)
              (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                                   object " matched an existing subject: " (.-s existing-flake) ".")
                              {:status 400 :error :db/invalid-tx :tempid _id}))

              ;; not a tempid, but subjects don't match
              ;; this can be OK assuming a different txi is retracting the existing flake
              ;; register a validating fn for post-processing to check this and throw if not the case
              (not= (.-s existing-flake) _id)
              (let [validating-fn (fn [{:keys [flakes] :as tx-state}]
                                    (let [check-flake (flake/flip-flake existing-flake t)
                                          retracted?  (contains? @flakes check-flake)]
                                      (when-not retracted?
                                        (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                                                             object " matched an existing subject: " (.-s existing-flake) ".")
                                                        {:status 400 :error :db/invalid-tx :tempid _id})))))]
                (register-validate-fn validating-fn false tx-state)
                (recur r)))))))))

(defn resolve-object-item
  "Resolves object into its final state so can be used for consistent comparisons with existing data."
  [object _id pred-info tx-state]
  (go-try
    (let [type    (pred-info :type)
          object* (if (txfunction/tx-fn? object)            ;; should only happen for multi-cardinality objects
                    (<? (txfunction/execute object _id pred-info tx-state))
                    object)]
      (cond
        (nil? object*) (throw (ex-info (str "Multi-cardinality values cannot be null/nil: " (pred-info :name))
                                       {:status 400 :error :db/invalid-tx}))

        (= :ref type) (cond
                        (tempid/TempId? object*) object*
                        (string? object*) (tempid/use object* tx-state)
                        (util/pred-ident? object*) (<? (resolve-ident-strict object* tx-state)))

        (= :tag type) (<? (tags/resolve object* pred-info tx-state))

        :else (conform-object-value object* type)))))

(defn resolve-object
  "Resolves object into its final state so can be used for consistent comparisons with existing data."
  [object _id pred-info tx-state]
  (let [multi? (pred-info :multi)]
    (cond-> object
            (nil? object) (async/go ::delete)
            (txfunction/tx-fn? object) (txfunction/execute _id pred-info tx-state)
            (not multi?) (resolve-object-item _id pred-info tx-state)
            multi? (->> (mapv #(resolve-object-item % _id pred-info tx-state))
                        async/merge
                        (async/into []))
            (pred-info :unique) (resolve-unique _id pred-info tx-state))))

(defn add-flake-to-txi
  "Used to add one or more proposed flakes to either temp-flakes or lookup-flakes"
  [txi [s p o t]]
  (let [flake (flake/->Flake s p o t true nil)]
    (if (or (tempid/TempId? s) (tempid/TempId? o))
      (update txi :_temp-flakes conj flake)
      (update txi :_lookup-flakes conj flake))))


(defn add-singleton-flake
  "Adds a new final flake, new-flake. A retract-flake (if not nil)
  is a matching flake in the existing db (i.e. a single-cardinality
  flake must retract an existing single-cardinality value if it already
  exists).

  Performs some logic to determine if the new flake should get added at
  all (i.e. if retract-flake is identical to the new flake)."
  [flakes ^Flake new-flake ^Flake retract-flake pred-info]
  (cond
    (nil? retract-flake)
    (conj flakes new-flake)

    ;; an existing flake is identical to this one, ignore unless :retractDuplicates is true
    (= (.-o new-flake) (.-o retract-flake))
    (if (pred-info :retractDuplicates)
      (conj flakes new-flake retract-flake)
      flakes)

    :else
    (conj flakes new-flake retract-flake)))


(defn generate-statements
  "Returns processed flakes into one of 3 buckets:
  - _final-flakes - Final, no need for additional processing
  - _temp-flakes - Single-cardinality flakes using a tempid. Must still resolve tempid and
                   if the tempid is resolved via a ':unique true' predicate
                   need to look up and retract any existing flakes with same subject+predicate
  - _temp-multi-flakes - multi-flakes that need permanent ids yet, but then act like _multi-flakes"
  [{:keys [_id _action _meta] :as txi} {:keys [db t] :as tx-state}]
  (go-try
    (let [_p-o-pairs (dissoc txi :_id :_action :_meta)
          _id*       (cond
                       (util/temp-ident? _id) (tempid/new (:_id txi) tx-state)
                       (util/pred-ident? _id) (<? (resolve-ident-strict _id tx-state))
                       :else _id)
          tempid?    (tempid/TempId? _id*)
          action     (resolve-action _action (empty? _p-o-pairs) tempid?)
          collection (resolve-collection-name _id* tx-state)]
      (if (and (= :delete action) (empty? _p-o-pairs))
        {:_final-flakes (<? (tx-retract/subject _id* tx-state))}
        (loop [acc {}
               [[pred obj] & r] _p-o-pairs]
          (if (nil? pred)                                   ;; finished
            acc
            (let [pred-info (predicate-details pred collection db)
                  pid       (pred-info :id)
                  obj*      (<? (resolve-object obj _id* pred-info tx-state))]
              (cond
                ;; delete should have no tempids, so can register the final flakes in tx-state
                (= :delete action)
                (-> acc
                    (update :_final-flakes into (if (pred-info :multi)
                                                  (<? (tx-retract/multi _id* pid obj* tx-state))
                                                  (<? (tx-retract/flake _id* pid obj* tx-state))))
                    (recur r))

                ;; multi could have a tempid as one of the values, need to look at each independently
                (pred-info :multi)
                (let [acc** (loop [acc* acc
                                   [o & r] obj*]
                              (if (nil? o)
                                acc*
                                (let [new-flake (flake/->Flake _id* pid o t true nil)]
                                  (if (or tempid? (tempid/TempId? o))
                                    (-> acc*
                                        (update :_temp-multi-flakes conj new-flake)
                                        (recur r))
                                    ;; multi-cardinality we only care if a flake matches exactly
                                    (let [retract-flake (first (<? (tx-retract/flake _id* pid obj tx-state)))
                                          final-flakes  (add-singleton-flake (:_final-flakes acc*) new-flake retract-flake pred-info)]
                                      (recur (assoc acc* :_final-flakes final-flakes) r))))))]
                  (recur acc** r))

                (or tempid? (tempid/TempId? obj*))
                (-> acc
                    (update :_temp-flakes conj (flake/->Flake _id* pid obj t true nil))
                    (recur r))

                ;; single-cardinality, and no tempid - we can make final and also do the lookup here
                ;; for a retraction flake, if present
                :else
                (let [new-flake     (flake/->Flake _id* pid obj t true nil)
                      ;; need to see if an existing flake exists that needs to get retracted
                      retract-flake (first (<? (tx-retract/flake _id* pid nil tx-state)))
                      final-flakes  (add-singleton-flake (:_final-flakes acc) new-flake retract-flake pred-info)]
                  (recur (assoc acc :_final-flakes final-flakes) r))))))))))


(defn- extract-children*
  "Takes a single transaction item (txi) and returns a two-tuple of
  [updated-txi nested-txi-list] if nested (children) transactions are found.
  If none found, will return [txi nil] where txi will be unaltered."
  [txi tx-state]
  (let [txi+tempid (if (util/temp-ident? (:_id txi))
                     (assoc txi :_id (tempid/new (:_id txi) tx-state))
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


(defn ->tx-state
  [db-before t auth authority block-instant {:keys [txid cmd sig nonce] :as tx-map}]
  {:db            db-before
   :db-root       (dbproto/-rootdb db-before)
   :db-after      db-before                                 ;; store updated db here
   :auth          auth
   :authority     authority
   :t             t
   :instant       block-instant
   :txid          txid
   :tx-string     cmd
   :signature     sig
   :nonce         nonce
   ;; hold map of all tempids to their permanent ids. After initial processing will use this to fill
   ;; all tempids with the permanent ids.
   :tempids       (atom {})
   ;; if a tempid resolves to existing subject via :upsert predicate, set it here. tempids don't need
   ;; to check for existing duplicate values, but if a tempid resolves via upsert, we need to check it
   :upserts       (atom nil)
   :fuel          (atom {:stack   []
                         :credits 1000000
                         :spent   0})

   ;; idents (two-tuples of unique predicate + value) may be used multiple times in same tx
   ;; we keep the ones we've already resolved here as a cache
   :idents        (atom {})                                 ;; cache of resolved identities
   ;; Unique predicate + value used in transaction kept here, to ensure the same unique is not used
   ;; multiple times within the transaction
   :uniques       (atom #{})
   ;; Some predicates may require extra validation after initial processing, we register functions
   ;; here for that purpose
   :validate-fn   (atom {:async nil :sync nil})             ;; put two flavors of validating functions in - async and sync

   ;; we may generate new tags as part of the transaction. Holds those new tags, but also a cache
   ;; of tag lookups to speed transaction by avoiding full lookups of the same tag multiple times in same tx
   :tags          (atom nil)
   ;; as data is getting processed, all schema flakes end up in this bucket to allow
   ;; for additional validation, and then processing prior to the other flakes.
   :schema-flakes (atom nil)
   })


(defn generate-permanent-ids
  [{:keys [db tempids upserts db-after t] :as tx-state} tx]
  (let [ecount (assoc (:ecount db) -1 t)]                   ;; make sure to set current _tx ecount to 't' value, even if no tempids in transaction
    (loop [[[tempid resolved-id] & r] @tempids
           tempids* @tempids
           upserts* #{}
           ecount*  ecount]
      (if (nil? tempid)                                     ;; finished
        (do (reset! tempids tempids*)
            (when-not (empty? upserts*)
              (reset! upserts upserts*))
            ;; return tx-state, don't need to update ecount in db-after, as dbproto/-with will update it
            tx-state)
        (if (nil? resolved-id)
          (let [cid      (dbproto/-c-prop db-after :id (:collection tempid))
                next-id  (if (= -1 cid)
                           t                                ; _tx collection has special handling as we decrement. Current value held in 't'
                           (-> ecount* (get cid) inc))
                ecount** (assoc ecount cid next-id)]
            (recur r (assoc tempids* tempid next-id) upserts* ecount**))
          (recur r tempids* (conj upserts* resolved-id) ecount*))))
    tx))

(defn resolve-temp-flakes
  [temp-flakes multi? {:keys [db tempids upserts] :as tx-state}]
  (go-try
    (loop [[^Flake tf & r] temp-flakes
           flakes []]
      (if (nil? tf)
        flakes
        (let [s     (.-s tf)
              o     (.-o tf)
              flake (cond-> tf
                            (tempid/TempId? s) (assoc :s (get @tempids s))
                            (tempid/TempId? o) (assoc :o (get @tempids o)))]
          (if (contains? @upserts s)                        ;; means tempid wasn't really a new subject but resolved to existing one somewhere in tx
            (if-let [retract-flake (first (<? (tx-retract/flake (.-s flake) (.-p tf) (if multi? (.-o flake) nil) tx-state)))]
              ;; if an existing matching flake exists, depending on equality and :retractDuplicates, may or may not
              ;; retract it, or might not even add the current flake
              (recur r (add-singleton-flake flakes flake retract-flake (fn [property] (dbproto/-p-prop db property (.-p flake)))))
              ;; no existing matching flake exists, always add
              (recur r (conj flakes flake)))
            (recur r (conj flakes flake))))))))


(defn special-subject?
  "Returns true if flake is considered a special subject, meaning it requires
  some extra validation to allow through."
  [^Flake flake]
  ;; this will capture any subject that is a collection, predicate, or tx-meta (< 0)
  (<= (.-s flake) schema-util/schema-sid-end))

;; TODO - predicate changes are handled at end, but what about validating
;; TODO - tx-functions and other items
(defn special-subject-handling
  "Handles additional validation or special treatment of mostly system collection subjects.
  All flakes must be of the same subject.

  Want to use this sparingly, ideally only validating flakes that we know need it."
  [tx-state flakes]
  (let [fflake (first flakes)]
    (cond
      (schema-util/is-tx-meta-flake? fflake)
      (do
        (doseq [^Flake flake flakes]
          (when
            (tx-meta/system-predicates (.-p flake))
            (throw (ex-info (str "Attempt to write a Fluree reserved predicate with id: " (.-p flake))
                            {:error  :db/invalid-transaction
                             :status 400}))))
        flakes)

      (schema-util/is-schema-flake? fflake)
      (do (swap! (:schema-flakes tx-state) into flakes)
          ;; don't return any schema flakes into mix, they will be added in downstream
          nil)

      :else flakes)))


(defn finalize-flakes
  [tx-state tx]
  (go-try
    (loop [[statements & r] tx
           flakes (flake/sorted-set-by flake/cmp-flakes-spot-novelty)]
      (if (nil? statements)
        flakes
        (let [{:keys [_temp-multi-flakes _temp-flakes _final-flakes]} statements
              temp       (when _temp-flakes
                           (<? (resolve-temp-flakes _temp-flakes false tx-state)))
              temp-multi (when _temp-multi-flakes
                           (<? (resolve-temp-flakes _temp-multi-flakes true tx-state)))
              new-flakes (concat _final-flakes temp temp-multi)]
          (if (special-subject? (first new-flakes))
            (->> new-flakes
                 (special-subject-handling tx-state)
                 (into flakes)
                 (recur r))
            (recur r (into flakes new-flakes))))))))


(defn validating-fns
  [tx-state flakes]
  (log/warn "validating-fns start: " flakes)
  flakes

  )

(defn do-transact
  [tx-state tx]
  (go-try
    (let [
          ;{:keys [tx-meta rest-tx sig txid authority auth]} tx-map
          ;tx-state (->tx-state db-before t auth block-instant) ;; holds state data for entire transaction
          ]
      (->> tx
           (mapcat #(extract-children % tx-state))
           ;; TODO - place into async pipeline
           (map #(generate-statements % tx-state))
           async/merge
           (async/into [])
           <?
           (generate-permanent-ids tx-state)
           (finalize-flakes tx-state)
           <?
           ;(validating-fns tx-state)
           ))))


(defn tempid-return-map
  "Creates a map of original user tempid strings to the resolved value."
  [{:keys [tempids] :as tx-state}]
  (reduce-kv (fn [acc tempid subject-id]
               (assoc acc (:user-string tempid) subject-id))
             {} @tempids))

(defn validate-db
  "Runs validations on permissions, attempts to do as much as possible in parallel."
  [candidate-db new-flakes tx-permissions {:keys [db tempids schema-flakes instant auth] :as tx-state}]
  (go-try
    (let [coll-spec-valid?-ch (tx-util/validate-collection-spec candidate-db new-flakes auth instant)
          pred-spec-valid?-ch (tx-util/validate-predicate-spec candidate-db new-flakes auth instant)
          valid-perm?-ch      (tx-util/validate-permissions db candidate-db new-flakes tx-permissions)]
      (when @schema-flakes
        ;; verify all schema changes are valid
        (<? (schema/validate-schema-change candidate-db @tempids @schema-flakes false)))
      (<? coll-spec-valid?-ch)
      (<? pred-spec-valid?-ch)
      (<? valid-perm?-ch)

      ;; return original db provided validation exception not thrown
      candidate-db)))

(defn build-transaction
  [_ db cmd-data next-t block-instant]
  (go-try
    (let [tx-map           (tx-util/validate-command (:command cmd-data))
          _                (when (not-empty (:deps tx-map)) ;; transaction has dependencies listed, verify they are satisfied
                             (or (<? (tx-util/deps-succeeded? db (:deps tx-map)))
                                 (throw (ex-info (str "One or more of the dependencies for this transaction failed: " (:deps tx-map))
                                                 {:status 403 :error :db/invalid-auth}))))
          {:keys [auth authority txid]} tx-map
          {:keys [auth-id authority-id tx-permissions]} (<? (tx-auth/resolve db auth authority))
          db-before        (assoc db :permissions tx-permissions)
          tx-state         (->tx-state db-before next-t auth-id authority-id block-instant tx-map)
          tx               (case (keyword (:type tx-map))   ;; command type is either :tx or :new-db
                             :tx (:tx tx-map)
                             :new-db (tx-util/create-new-db-tx tx-map))
          tx-flakes        (<? (do-transact tx-state tx))
          tx-meta-flakes   (tx-meta/tx-meta-flakes tx-state nil)

          schema-flakes    @(:schema-flakes tx-state)
          all-flakes       (cond-> (into tx-flakes tx-meta-flakes)
                                   schema-flakes (into schema-flakes)
                                   @(:tags tx-state) (into (tags/create-flakes tx-state)))

          ;; kick off hash process in the background, it can take a while
          hash-flake       (future (tx-meta/generate-hash-flake all-flakes tx-state))
          fast-forward-db? (:tt-id db-before)
          ;; final db that can be used for any final testing/spec validation
          db-after         (-> (if fast-forward-db?
                                 (<? (dbproto/-forward-time-travel db-before all-flakes))
                                 (<? (dbproto/-with-t db-before all-flakes)))
                               dbproto/-rootdb
                               tx-util/make-candidate-db
                               (tx-meta/add-tx-hash-flake @hash-flake))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-before [:stats :size]))]

      ;; run all validations
      (<? (validate-db db-after all-flakes tx-permissions tx-state))

      ;; note here that db-after does NOT contain the tx-hash flake. The hash generation takes time, so is done in the background
      ;; This should not matter as
      {:t            next-t
       :hash         (.-o @hash-flake)
       :db-after     db-after
       :flakes       (conj all-flakes @hash-flake)
       :tempids      (tempid-return-map tx-state)
       :bytes        tx-bytes
       :fuel         (+ (:spent @(:fuel tx-state)) tx-bytes (count all-flakes) 1)
       :status       200
       :txid         txid
       :auth         auth
       :authority    authority
       :type         (keyword (:type tx-map))
       :remove-preds (when schema-flakes
                       (schema-util/remove-from-post-preds schema-flakes))})))

(comment
  (def conn (:conn user/system))
  (def db (async/<!! (fluree.db.api/db conn "prefix/a")))
  (def last-resp nil)

  (time (let [test-tx (fluree.db.api/tx->command "prefix/a"
                                                 [{:_id   ["movie/title" "Gran Torino"]
                                                   :title "Gran Torino2"}]
                                                 "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957")
              res     (-> (build-transaction nil db {:command test-tx} -9 (java.time.Instant/now))
                          (async/<!!))]
          (alter-var-root #'fluree.db.ledger.transact.json/last-resp (constantly res))
          res))

  (def test-tx (fluree.db.api/tx->command "prefix/a"
                                          [{:_id   ["movie/title" "Gran Torino"]
                                            :title "Gran Torino2"}]
                                          "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957"))
  (time (async/<!! (build-transaction nil db {:command test-tx} -9 (java.time.Instant/now))))

  (-> last-resp
      :db-after
      (async/go)
      (fluree.db.api/query {:select [:*] :from "movie"})
      (deref))

  )



(comment

  (require 'clojure.core.async :refer :all)

  (def ca (async/chan 1))
  (def cb (async/chan 1))

  (pipeline
    4                                                       ; thread count, i prefer egyptian cotton
    cb                                                      ; to
    (filter even?)                                          ; transducer
    ca                                                      ; from
    )

  (doseq [i (range 10)]
    (async/go (clojure.core.async/>! ca i)))
  (async/go-loop []
    (println (async/<! cb))
    (recur))

  (async/close! ca)
  (async/close! cb)

  (inc 1)

  )



