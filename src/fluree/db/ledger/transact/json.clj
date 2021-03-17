(ns fluree.db.ledger.transact.json
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
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
            [fluree.db.ledger.transact.retract :as tx-retract]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.ledger.transact.tags :as tags]
            [fluree.db.ledger.transact.txfunction :as txfunction]
            [fluree.db.ledger.transact.auth :as tx-auth]
            [fluree.db.ledger.transact.tx-meta :as tx-meta]
            [fluree.db.ledger.transact.validation :as tx-validate]
            [fluree.db.ledger.transact.error :as tx-error])
  (:import (fluree.db.flake Flake)))


(def ^:const parallelism
  "Processes this many transaction items in parallel."
  8)

(defn register-validate-fn
  [f {:keys [validate-fn] :as tx-state}]
  (swap! validate-fn update :queue conj f))

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
  [_id {:keys [db-root] :as tx-state}]
  (cond (tempid/TempId? _id)
        (:collection _id)

        (neg-int? _id)
        "_tx"

        (int? _id)
        (->> (flake/sid->cid _id)
             (dbproto/-c-prop db-root :name))))


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
    (fspec/type-check object type)))


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
  [object-ch _id pred-info {:keys [db-before t] :as tx-state}]
  (go-try
    (let [object       (<? object-ch)
          pred-id      (pred-info :id)
          ;; create a two-tuple of [object-value async-chan-with-found-subjects]
          ;; to iterate over. Kicks off queries (if multi) in parallel.
          obj+subj-chs (->> (if (pred-info :multi) object [object])
                            (map #(vector % (query-range/index-range db-before :post = [pred-id %]))))]
      ;; use a loop here so we can use async to full extent
      (loop [[[obj subject-ch] & r] obj+subj-chs]
        (if-not obj
          ;; finished, return original object - no errors
          object
          ;; check out next ident
          (let [^Flake existing-flake (first (<? subject-ch))]
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
              (let [validate-subject-retracted (fn [{:keys [flakes] :as tx-state}]
                                                 (let [check-flake (flake/flip-flake existing-flake t)
                                                       retracted?  (contains? flakes check-flake)]
                                                   (when-not retracted?
                                                     (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                                                                          object " matched an existing subject: " (.-s existing-flake) ".")
                                                                     {:status 400 :error :db/invalid-tx :tempid _id})))))]
                (register-validate-fn validate-subject-retracted tx-state)
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
                        (int? object*) (<? (resolve-ident-strict object* tx-state))
                        (util/pred-ident? object*) (<? (resolve-ident-strict object* tx-state)))

        (= :tag type) (<? (tags/resolve object* pred-info tx-state))

        :else (conform-object-value object* type)))))


(defn resolve-object
  "Resolves object into its final state so can be used for consistent comparisons with existing data."
  [object _id pred-info tx-state]
  (let [multi? (pred-info :multi)]
    (cond-> object
            (nil? object) (async/go ::delete)
            (not multi?) (resolve-object-item _id pred-info tx-state)
            multi? (->> (mapv #(resolve-object-item % _id pred-info tx-state))
                        async/merge
                        (async/into []))
            (pred-info :unique) (resolve-unique _id pred-info tx-state))))


(defn add-singleton-flake
  "Adds new-flake assuming not a duplicate. A retract-flake (if not nil)
  is a matching flake in the existing db (i.e. a single-cardinality
  flake must retract an existing single-cardinality value if it already
  exists).

  Performs some logic to determine if the new flake should get added at
  all (i.e. if retract-flake is identical to the new flake)."
  [flakes ^Flake new-flake ^Flake retract-flake pred-info tx-state]
  (cond
    ;; no retraction flake, always add
    (nil? retract-flake)
    (cond-> (conj flakes new-flake)
            (pred-info :spec) (tx-validate/queue-pred-spec new-flake pred-info tx-state)
            (pred-info :txSpec) (tx-validate/queue-predicate-tx-spec [new-flake] pred-info tx-state))

    ;; new and retraction flake are identical
    (= (.-o new-flake) (.-o retract-flake))
    (if (pred-info :retractDuplicates)
      ;; no need for predicate spec, new flake is exactly same as old
      (cond-> (conj flakes new-flake retract-flake)
              (pred-info :txSpec) (tx-validate/queue-predicate-tx-spec [new-flake retract-flake] pred-info tx-state))
      ;; don't add new or retract flake
      flakes)

    ;; new and retraction flakes are different
    :else
    (cond-> (conj flakes new-flake retract-flake)
            (pred-info :spec) (tx-validate/queue-pred-spec new-flake pred-info tx-state)
            (pred-info :txSpec) (tx-validate/queue-predicate-tx-spec [new-flake retract-flake] pred-info tx-state))))


(defn generate-statements
  "Returns processed flakes into one of 3 buckets:
  - _final-flakes - Final, no need for additional processing
  - _temp-flakes - Single-cardinality flakes using a tempid. Must still resolve tempid and
                   if the tempid is resolved via a ':unique true' predicate
                   need to look up and retract any existing flakes with same subject+predicate
  - _temp-multi-flakes - multi-flakes that need permanent ids yet, but then act like _multi-flakes"
  [{:keys [_id _action _meta] :as txi} {:keys [db-before t] :as tx-state}]
  (go-try
    (let [_p-o-pairs (dissoc txi :_id :_action :_meta)
          _id*       (cond
                       (util/temp-ident? _id) (tempid/new (:_id txi) tx-state)
                       (util/pred-ident? _id) (<? (resolve-ident-strict _id tx-state))
                       :else _id)
          tempid?    (tempid/TempId? _id*)
          action     (resolve-action _action (empty? _p-o-pairs) tempid?)
          collection (resolve-collection-name _id* tx-state)
          txi*       (assoc txi :_collection collection)]
      (if (and (= :delete action) (empty? _p-o-pairs))
        (assoc txi* :_final-flakes (<? (tx-retract/subject _id* tx-state)))
        (loop [acc txi*
               [[pred obj] & r] _p-o-pairs]
          (if (nil? pred)                                   ;; finished
            acc
            (let [pred-info (predicate-details pred collection db-before)
                  pid       (pred-info :id)
                  obj*      (if (txfunction/tx-fn? obj)
                              (-> (txfunction/execute obj _id pred-info tx-state)
                                  <?
                                  (resolve-object _id* pred-info tx-state)
                                  <?)
                              (<? (resolve-object obj _id* pred-info tx-state)))]
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
                                    (let [retract-flake (first (<? (tx-retract/flake _id* pid o tx-state)))
                                          final-flakes  (add-singleton-flake (:_final-flakes acc*) new-flake retract-flake pred-info tx-state)]
                                      (recur (assoc acc* :_final-flakes final-flakes) r))))))]
                  (recur acc** r))

                (or tempid? (tempid/TempId? obj*))
                (-> acc
                    (update :_temp-flakes conj (flake/->Flake _id* pid obj* t true nil))
                    (recur r))

                ;; single-cardinality, and no tempid - we can make final and also do the lookup here
                ;; for a retraction flake, if present
                :else
                (let [new-flake     (flake/->Flake _id* pid obj* t true nil)
                      ;; need to see if an existing flake exists that needs to get retracted
                      retract-flake (first (<? (tx-retract/flake _id* pid nil tx-state)))
                      final-flakes  (add-singleton-flake (:_final-flakes acc) new-flake retract-flake pred-info tx-state)]
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
  [db t block-instant {:keys [auth auth-sid authority authority-sid tx-permissions txid cmd sig nonce type] :as tx-map}]
  (let [tx        (case (keyword (:type tx-map))            ;; command type is either :tx or :new-db
                    :tx (:tx tx-map)
                    :new-db (tx-util/create-new-db-tx tx-map))
        db-before (cond-> db
                          tx-permissions (assoc :permissions tx-permissions))]
    {:db-before     db-before
     :db-root       db
     :db-after      (atom nil)                              ;; store updated db here
     :flakes        nil                                     ;; holds final list of flakes for tx once complete
     :auth-id       auth                                    ;; auth id string in _auth/id
     :auth          auth-sid                                ;; auth subject-id integer
     :authority-id  authority                               ;; authority id string as per _auth/id (or nil if no authority)
     :authority     authority-sid                           ;; authority subject-id integer (or nil if no authority)
     :t             t
     :instant       block-instant
     :txid          txid
     :tx-type       type
     :tx            tx
     :tx-string     cmd
     :signature     sig
     :nonce         nonce
     :fuel          (atom {:stack   []
                           :credits 1000000
                           :spent   0})
     ;; hold map of all tempids to their permanent ids. After initial processing will use this to fill
     ;; all tempids with the permanent ids.
     :tempids       (atom {})
     ;; idents (two-tuples of unique predicate + value) may be used multiple times in same tx
     ;; we keep the ones we've already resolved here as a cache
     :idents        (atom {})
     ;; if a tempid resolves to existing subject via :upsert predicate, set it here. tempids don't need
     ;; to check for existing duplicate values, but if a tempid resolves via upsert, we need to check it
     :upserts       (atom nil)                              ;; cache of resolved identities
     ;; Unique predicate + value used in transaction kept here, to ensure the same unique is not used
     ;; multiple times within the transaction
     :uniques       (atom #{})

     ;; as data is getting processed, all schema flakes end up in this bucket to allow
     ;; for additional validation, and then processing prior to the other flakes.
     :schema-flakes (atom nil)
     ;; we may generate new tags as part of the transaction. Holds those new tags, but also a cache
     ;; of tag lookups to speed transaction by avoiding full lookups of the same tag multiple times in same tx
     :tags          (atom nil)

     ;; Some predicates may require extra validation after initial processing, we register functions
     ;; here for that purpose, 'cache' holds cached functions that are ready to execute
     :validate-fn   (atom {:queue   [] :cache {}
                           ;; need to track respective flakes for predicates (for tx-spec) and subject changes (collection-specs)
                           :tx-spec nil :c-spec nil})}))


(defn assign-permanent-ids
  "Assigns any unresolved tempids with a permanent subject id."
  [{:keys [tempids upserts db-before t] :as tx-state} tx]
  (try
    (let [ecount (assoc (:ecount db-before) -1 t)]          ;; make sure to set current _tx ecount to 't' value, even if no tempids in transaction
      (loop [[[tempid resolved-id] & r] @tempids
             tempids* @tempids
             upserts* #{}
             ecount*  ecount]
        (if (nil? tempid)                                   ;; finished
          (do (reset! tempids tempids*)
              (when-not (empty? upserts*)
                (reset! upserts upserts*))
              ;; return tx-state, don't need to update ecount in db-after, as dbproto/-with will update it
              tx-state)
          (if (nil? resolved-id)
            (let [cid      (dbproto/-c-prop db-before :id (:collection tempid))
                  next-id  (if (= -1 cid)
                             t                              ; _tx collection has special handling as we decrement. Current value held in 't'
                             (if-let [last-sid (get ecount* cid)]
                               (inc last-sid)
                               (flake/->sid cid 0)))
                  ecount** (assoc ecount* cid next-id)]
              (recur r (assoc tempids* tempid next-id) upserts* ecount**))
            (recur r tempids* (conj upserts* resolved-id) ecount*))))
      tx)
    (catch Exception e
      (log/error e (str "Unexpected error assigning permanent id to tempids."
                        "with error: " (.getMessage e))
                 {:ecount      (:ecount db-before)
                  :tempids     @tempids
                  :schema-coll (get-in db-before [:schema :coll])})
      (throw (ex-info (str "Unexpected error assigning permanent id to tempids."
                           "with error: " (.getMessage e))
                      {:status 500 :error :db/unexpected-error}
                      e)))))


(defn resolve-temp-flakes
  [temp-flakes multi? {:keys [db-before tempids upserts] :as tx-state}]
  (go-try
    (loop [[^Flake tf & r] temp-flakes
           flakes []]
      (if (nil? tf)
        flakes
        (let [s             (.-s tf)
              o             (.-o tf)
              ^Flake flake  (cond-> tf
                                    (tempid/TempId? s) (assoc :s (get @tempids s))
                                    (tempid/TempId? o) (assoc :o (get @tempids o)))
              retract-flake (when (contains? @upserts s)    ;; if was an upsert resolved, could be an existing flake that needs to get retracted
                              (first (<? (tx-retract/flake (.-s flake) (.-p tf) (if multi? (.-o flake) nil) tx-state))))
              pred-info     (fn [property] (dbproto/-p-prop db-before property (.-p flake)))
              flakes*       (add-singleton-flake flakes flake retract-flake pred-info tx-state)]
          (recur r flakes*))))))


(defn finalize-flakes
  [tx-state tx]
  (go-try
    (loop [[statements & r] tx
           flakes (flake/sorted-set-by flake/cmp-flakes-block)]
      (if (nil? statements)
        flakes
        (let [{:keys [_temp-multi-flakes _temp-flakes _final-flakes _collection]} statements
              temp       (when _temp-flakes
                           (<? (resolve-temp-flakes _temp-flakes false tx-state)))
              temp-multi (when _temp-multi-flakes
                           (<? (resolve-temp-flakes _temp-multi-flakes true tx-state)))
              new-flakes (concat _final-flakes temp temp-multi)]
          (if (empty? new-flakes)
            (recur r flakes)
            (->> new-flakes
                 (tx-validate/check-collection-specs _collection tx-state) ;; returns original flakes, but registers collection spec for execution if applicable
                 (into flakes)
                 (recur r))))))))


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
           (assign-permanent-ids tx-state)
           (finalize-flakes tx-state)
           <?))))


(defn update-db-after
  "Updates db-after into tx-state"
  [db-after tx-state]
  (reset! (:db-after tx-state) db-after)
  db-after)


(defn build-transaction
  [tx-state]
  (go-try
    (let [{:keys [db-before auth-id authority-id txid tx t tx-type fuel]} tx-state
          tx-flakes        (<? (do-transact tx-state tx))
          tx-meta-flakes   (tx-meta/tx-meta-flakes tx-state nil)
          tempids-map      (tempid/result-map tx-state)
          schema-flakes    @(:schema-flakes tx-state)
          all-flakes       (cond-> (into tx-flakes tx-meta-flakes)
                                   schema-flakes (into schema-flakes)
                                   (not-empty tempids-map) (conj (tempid/flake tempids-map t))
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
                               (tx-meta/add-tx-hash-flake @hash-flake)
                               (update-db-after tx-state))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-before [:stats :size]))]

      ;; runs all 'spec' validations
      (<? (tx-validate/run all-flakes tx-state parallelism))

      (<? (tx-validate/permissions db-before db-after all-flakes))

      ;; note here that db-after does NOT contain the tx-hash flake. The hash generation takes time, so is done in the background
      ;; This should not matter as
      {:t            t
       :hash         (.-o ^Flake @hash-flake)
       :db-before    db-before
       :db-after     db-after
       :flakes       (conj all-flakes @hash-flake)
       :tempids      tempids-map
       :bytes        tx-bytes
       :fuel         (+ (:spent @fuel) tx-bytes (count all-flakes) 1)
       :status       200
       ;:duration     (str (- (System/currentTimeMillis) start-time) "ms")
       :txid         txid
       :auth         auth-id
       :authority    authority-id
       :type         tx-type
       :remove-preds (when schema-flakes
                       (schema-util/remove-from-post-preds schema-flakes))})))


(defn transact
  [db cmd-data t block-instant]
  (async/go
    (try
      (let [tx-map   (try (tx-util/validate-command (:command cmd-data))
                          (catch Exception e
                            (log/error e "Unexpected error parsing command: " (pr-str cmd-data))
                            (throw (ex-info "Unexpected error parsing command."
                                            {:status   500
                                             :error    :db/command-parse-exception
                                             :cmd-data cmd-data}
                                            e))))
            _        (when (not-empty (:deps tx-map))       ;; transaction has dependencies listed, verify they are satisfied
                       (<? (tx-validate/tx-deps-check db tx-map)))
            tx-map*  (<? (tx-auth/add-auth-ids-permissions db tx-map))
            tx-state (->tx-state db t block-instant tx-map*)
            result   (async/<! (build-transaction tx-state))]
        (if (util/exception? result)
          (<? (tx-error/handler result tx-state))
          result))
      (catch Exception e
        (async/<! (tx-error/pre-processing-handler e db cmd-data t))))))



(comment
  (def conn (:conn user/system))
  (def db (async/<!! (fluree.db.api/db conn "blank/ledger2")))
  (def last-resp nil)

  (-> db
      :schema
      :pred
      keys)

  ;(def newdb (async/<!! (dbproto/-with-t db #{#Flake [17592186044436 40 "person" -3 true nil] #Flake [-3 100 "853204521965754426ebf18ec11cd457ed587f03d1b6d54ef5d82ff58bc1ba45" -3 true nil] #Flake [-3 101 105553116266496 -3 true nil] #Flake [-3 103 1615308222639 -3 true nil] #Flake [-3 106 "{\"type\":\"tx\",\"db\":\"blank/ledger\",\"tx\":[{\"_id\":\"_collection\",\"name\":\"person\"}],\"nonce\":1615308222639,\"auth\":\"TfE9koGjRzWrWD9VbqN3KdyPZor1eDrafmU\",\"expire\":1615308252639}" -3 true nil] #Flake [-3 107 "1b304402207db6c8feb3c26822e4133850ce3425c50124beaf10a907cfffe60574be83c7ce02202a7ab67b66f8d109d97b53f06d4f659b144d1753bf6ae5c983db5abd9c371b0d" -3 true nil]})))
  newdb

  (time (fluree.db.api/tx->command
          "blank/ledger2"
          [{:_id "_collection", :name "person"}
           {:_id "_collection", :name "chat"}
           {:_id "_collection", :name "comment"}
           {:_id "_collection", :name "artist"}
           {:_id "_collection", :name "movie"}
           {:_id "_predicate", :name "person/handle", :doc "The person's unique handle", :unique true, :type "string"}
           {:_id "_predicate", :name "person/fullName", :doc "The person's full name.", :type "string", :index true}
           {:_id "_predicate", :name "person/age", :doc "The person's age in years", :type "int", :index true}
           {:_id "_predicate", :name "person/follows", :doc "Any persons this subject follows", :type "ref", :restrictCollection "person"}
           {:_id "_predicate", :name "person/favNums", :doc "The person's favorite numbers", :type "int", :multi true}
           {:_id "_predicate", :name "person/favArtists", :doc "The person's favorite artists", :type "ref", :restrictCollection "artist", :multi true}
           {:_id "_predicate", :name "person/favMovies", :doc "The person's favorite movies", :type "ref", :restrictCollection "movie", :multi true}
           {:_id "_predicate", :name "person/user", :type "ref", :restrictCollection "_user"}
           {:_id "_predicate", :name "chat/message", :doc "A chat message", :type "string", :fullText true}
           {:_id "_predicate", :name "chat/person", :doc "A reference to the person that created the message", :type "ref", :restrictCollection "person"}
           {:_id "_predicate", :name "chat/instant", :doc "The instant in time when this chat happened.", :type "instant", :index true}
           {:_id "_predicate", :name "chat/comments", :doc "A reference to comments about this message", :type "ref", :component true, :multi true, :restrictCollection "comment"}
           {:_id "_predicate", :name "comment/message", :doc "A comment message.", :type "string", :fullText true}
           {:_id "_predicate", :name "comment/person", :doc "A reference to the person that made the comment", :type "ref", :restrictCollection "person"}
           {:_id "_predicate", :name "artist/name", :type "string", :unique true}
           {:_id "_predicate", :name "movie/title", :type "string", :fullText true, :unique true}
           ]
          "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957"))

  (time (let [db      db
              test-tx (fluree.db.api/tx->command
                        "blank/ledger2"
                        [{:_id "_collection", :name "person"}
                         {:_id "_collection", :name "chat"}
                         {:_id "_collection", :name "comment"}
                         {:_id "_collection", :name "artist"}
                         {:_id "_collection", :name "movie"}
                         {:_id "_predicate", :name "person/handle", :doc "The person's unique handle", :unique true, :type "string"}
                         {:_id "_predicate", :name "person/fullName", :doc "The person's full name.", :type "string", :index true}
                         {:_id "_predicate", :name "person/age", :doc "The person's age in years", :type "int", :index true}
                         {:_id "_predicate", :name "person/follows", :doc "Any persons this subject follows", :type "ref", :restrictCollection "person"}
                         {:_id "_predicate", :name "person/favNums", :doc "The person's favorite numbers", :type "int", :multi true}
                         {:_id "_predicate", :name "person/favArtists", :doc "The person's favorite artists", :type "ref", :restrictCollection "artist", :multi true}
                         {:_id "_predicate", :name "person/favMovies", :doc "The person's favorite movies", :type "ref", :restrictCollection "movie", :multi true}
                         {:_id "_predicate", :name "person/user", :type "ref", :restrictCollection "_user"}
                         {:_id "_predicate", :name "chat/message", :doc "A chat message", :type "string", :fullText true}
                         {:_id "_predicate", :name "chat/person", :doc "A reference to the person that created the message", :type "ref", :restrictCollection "person"}
                         {:_id "_predicate", :name "chat/instant", :doc "The instant in time when this chat happened.", :type "instant", :index true}
                         {:_id "_predicate", :name "chat/comments", :doc "A reference to comments about this message", :type "ref", :component true, :multi true, :restrictCollection "comment"}
                         {:_id "_predicate", :name "comment/message", :doc "A comment message.", :type "string", :fullText true}
                         {:_id "_predicate", :name "comment/person", :doc "A reference to the person that made the comment", :type "ref", :restrictCollection "person"}
                         {:_id "_predicate", :name "artist/name", :type "string", :unique true}
                         {:_id "_predicate", :name "movie/title", :type "string", :fullText true, :unique true}
                         ]
                        "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957")
              res     (-> (build-transaction nil db {:command test-tx} (dec (:t db)) (java.time.Instant/now))
                          (async/<!!))]
          (alter-var-root #'fluree.db.ledger.transact.json/last-resp (constantly res))
          res))

  (-> last-resp
      :db-after
      (async/go)
      (fluree.db.api/query {:select [:*] :from "_predicate"})
      deref
      )

  (criterium.core/quick-bench
    (let [test-tx (fluree.db.api/tx->command "prefix/a"
                                             [{:_id   ["movie/title" "Gran Torino"]
                                               :title "Gran Torino2"}
                                              {:_id   "movie"
                                               :title "New Movie"}
                                              {:_id   "movie"
                                               :title "New Movie2"}
                                              {:_id  "artist"
                                               :name "Brian"}]
                                             "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957")
          res     (-> (build-transaction nil db {:command test-tx} (dec (:t db)) (java.time.Instant/now))
                      (async/<!!))]
      ;(alter-var-root #'fluree.db.ledger.transact.json/last-resp (constantly res))
      res))


  (criterium.core/quick-bench
    (let [test-tx (fluree.db.api/tx->command "prefix/a"
                                             [{:_id   ["movie/title" "Gran Torino"]
                                               :title "Gran Torino2"}
                                              {:_id   "movie"
                                               :title "New Movie"}
                                              {:_id   "movie"
                                               :title "New Movie2"}
                                              {:_id  "artist"
                                               :name "Brian"}]
                                             "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957")
          res     (-> (fluree.db.ledger.transact/build-transaction nil db {:command test-tx} (dec (:t db)) (java.time.Instant/now))
                      (async/<!!))]
      ;(alter-var-root #'fluree.db.ledger.transact.json/last-resp (constantly res))
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



