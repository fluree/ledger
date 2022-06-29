(ns fluree.db.ledger.transact.core
  (:require [fluree.db.util.async :refer [<? go-try]]
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
            [fluree.db.ledger.transact.retract :as tx-retract]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.ledger.transact.tags :as tags]
            [fluree.db.ledger.transact.txfunction :as txfunction]
            [fluree.db.ledger.transact.auth :as tx-auth]
            [fluree.db.ledger.transact.tx-meta :as tx-meta]
            [fluree.db.ledger.transact.validation :as tx-validate]
            [fluree.db.ledger.transact.error :as tx-error]
            [fluree.db.ledger.transact.schema :as tx-schema])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)


(def ^:const parallelism
  "Processes this many transaction items in parallel."
  8)

(defn register-validate-fn
  [f {:keys [validate-fn]}]
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
          [tempid (assoc predicate-value :_id tempid)])))


(defn resolve-ident-strict
  "Resolves ident (from cache if exists). Will throw exception if ident cannot be resolved."
  [ident {:keys [db-root idents]}]
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
  [_id {:keys [db-root]}]
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


(defn register-unique!
  "Registers unique value in the tx-state and return true if successful.
  Will be unsuccessful if the ident already exists in the unique value cache
  and return false.

  Ident is a two-tuple of [pred-id object/value]"
  [ident {:keys [uniques]}]
  ;; uniques is a set/#{} wrapped in an atom
  (let [uniques* (swap! uniques
                        (fn [uniques-set]
                          (if (contains? uniques-set ident)
                            (conj uniques-set ::duplicate-detected) ;; check for this special keyword in result
                            (conj uniques-set ident))))]
    (if (contains? uniques* ::duplicate-detected)
      false                                                 ;; already registered, should throw downstream
      true)))


(defn resolve-unique
  "If predicate is unique, need to determine if any matching values already exist both
  in existing ledger, but also within the transaction.

  If they exist in the existing ledger, but upsert? is true, we can resolve the
  tempid to subject id (but only if a different upsert? predicate didn't already
  resolve it to a different subject!)

  If error is not thrown, returns the provided object argument."
  [object-ch _id pred-info {:keys [db-before] :as tx-state}]
  (go-try
    (let [object       (<? object-ch)
          pred-id      (pred-info :id)
          ;; create a two-tuple of [object-value async-chan-with-found-subjects]
          ;; to iterate over. Kicks off queries (if multi) in parallel.
          obj+subj-chs (->> (if (pred-info :multi) object [object])
                            (map #(vector % (if (tempid/TempId? %)
                                              (async/go [%]) ;; can't look up tempids, validate they were not resolved as final tx step (see validating fn below)
                                              (query-range/index-range db-before :post = [pred-id %])))))]
      ;; use a loop here so we can use async to full extent
      (loop [[[obj subject-ch] & r] obj+subj-chs]
        (when obj
          ;; register every unique pred+object combo into state to ensure not used multiple times in same transaction
          (when (false? (register-unique! [pred-id obj] tx-state))
            (throw (ex-info (str "Unique predicate " (pred-info :name) " was used more than once "
                                 "in the transaction with the value of: " object ".")
                            {:status 400 :error :db/invalid-tx}))))
        (if-not obj
          ;; finished, return original object - no errors
          object
          ;; check out next ident, if existing-flake we have a potential conflict
          (let [existing-flake (first (<? subject-ch))]
            (cond
              ;; if a tempid, just need to make sure (a) only used once [done by register-unique!]
              ;; (b) didn't resolve to existing subject which we'd have to check at final tx result - done here by registering validation-fn
              (tempid/TempId? existing-flake)
              (do
                (tx-validate/queue-check-unique-tempid-still-unique existing-flake _id pred-info tx-state)
                (recur r))


              ;; no matching existing flake, move on
              (nil? existing-flake) (recur r)

              ;; lookup subject matches subject, will end up ignoring insert downstream unless :retractDuplicates is true
              (= (:s existing-flake) _id) (recur r)

              ;; found existing subject and tempid, so set tempid value (or throw if already set to different subject)
              (and (tempid/TempId? _id) (pred-info :upsert))
              (do
                (tempid/set _id (:s existing-flake) tx-state) ;; will throw if tempid was already set to a different subject
                (recur r))

              ;; tempid, but not upsert - throw
              (tempid/TempId? _id)
              (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                                   object " matched an existing subject: " (:s existing-flake) ".")
                              {:status 400 :error :db/invalid-tx :tempid _id}))

              ;; not a tempid, but subjects don't match
              ;; this can be OK assuming a different txi is retracting the existing flake
              ;; register a validating fn for post-processing to check this and throw if not the case
              (not= (:s existing-flake) _id)
              (do
                (tx-validate/queue-check-unique-match-retracted existing-flake _id pred-info object tx-state)
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
    (if (nil? object)
      (async/go :delete)                                    ;; delete any existing object
      (cond-> object
              (not multi?) (resolve-object-item _id pred-info tx-state)
              multi? (#(if (sequential? %) % [%]))
              multi? (->> (mapv #(resolve-object-item % _id pred-info tx-state))
                          async/merge
                          (async/into []))
              (pred-info :unique) (resolve-unique _id pred-info tx-state)))))


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
  [{:keys [db-before t tempids upserts] :as tx-state}
   {:keys [_id _action _meta] :as txi} res-chan]
  (async/go
    (try
      (let [_p-o-pairs (dissoc txi :_id :_action :_meta)
            _id*       (if (util/pred-ident? _id)
                         (<? (resolve-ident-strict _id tx-state))
                         _id)
            delete?    (when _action (or (= :delete _action) (= "delete" _action)))
            collection (resolve-collection-name _id* tx-state)
            txi*       (assoc txi :_collection collection)
            pi-obj     (loop [acc []
                              [[pred obj] & r] _p-o-pairs]
                         (if (nil? pred)
                           acc
                           (let [pred-info (predicate-details pred collection db-before)
                                 obj*      (if (txfunction/tx-fn? obj)
                                             (-> (txfunction/execute obj _id pred-info tx-state)
                                                 <?
                                                 (resolve-object _id* pred-info tx-state)
                                                 <?)
                                             (<? (resolve-object obj _id* pred-info tx-state)))]
                             (recur (conj acc [pred-info obj*]) r))))
            _id**      (or (get @tempids _id*) _id*)        ;; if a ':upsert true' value resolved to existing sid, will now be in @tempids map
            tempid?    (tempid/TempId? _id**)]
        (when (and delete? tempid?)
          (throw (ex-info (str "Tempid with a 'delete' action is not allowed: " _id)
                          {:status 400 :error :db/invalid-transaction})))
        (if (and delete? (empty? _p-o-pairs))
          (async/>! res-chan (assoc txi* :_final-flakes (<? (tx-retract/subject _id** tx-state))))
          (loop [acc txi*
                 [[pred-info obj*] & r] pi-obj]
            (if (nil? pred-info)                            ;; finished
              (async/>! res-chan acc)
              (let [pid (pred-info :id)]
                (cond
                  ;; deletion/nil - if tempid, then just ignore, else remove all existing values for s+p
                  (= :delete obj*)
                  (if tempid?
                    (recur acc r)
                    (-> acc
                        (update :_final-flakes into (<? (tx-retract/flake _id** pid nil tx-state)))
                        (recur r)))

                  ;; delete should have no tempids, so can register the final flakes in tx-state
                  delete?
                  (-> acc
                      (update :_final-flakes into (if (pred-info :multi)
                                                    (<? (tx-retract/multi _id** pid obj* tx-state))
                                                    (<? (tx-retract/flake _id** pid obj* tx-state))))
                      (recur r))

                  ;; multi could have a tempid as one of the values, need to look at each independently
                  (pred-info :multi)
                  (let [acc** (loop [acc* acc
                                     [o & r] obj*]
                                (cond
                                  (nil? o) acc*
                                  (util/exception? o) (throw o)
                                  :else (let [new-flake (flake/->Flake _id** pid o t true nil)]
                                          (if (or tempid? (tempid/TempId? o))
                                            (-> acc*
                                                (update :_temp-multi-flakes conj new-flake)
                                                (recur r))
                                            ;; multi-cardinality we only care if a flake matches exactly
                                            (let [retract-flake (first (<? (tx-retract/flake _id** pid o tx-state)))
                                                  final-flakes  (add-singleton-flake (:_final-flakes acc*) new-flake retract-flake pred-info tx-state)]
                                              (recur (assoc acc* :_final-flakes final-flakes) r))))))]
                    (recur acc** r))

                  (or tempid? (tempid/TempId? obj*))
                  (do
                    (swap! upserts conj _id**) ; make sure we retract the old value
                    (-> acc
                        (update :_temp-flakes conj (flake/->Flake _id** pid obj* t true nil))
                        (recur r)))

                  ;; single-cardinality, and no tempid - we can make final and also do the lookup here
                  ;; for a retraction flake, if present
                  :else
                  (let [new-flake     (flake/->Flake _id** pid obj* t true nil)
                        ;; need to see if an existing flake exists that needs to get retracted
                        retract-flake (first (<? (tx-retract/flake _id** pid nil tx-state)))
                        final-flakes  (add-singleton-flake (:_final-flakes acc) new-flake retract-flake pred-info tx-state)]
                    (recur (assoc acc :_final-flakes final-flakes) r)))))))
        (async/close! res-chan))
      (catch Exception e (async/put! res-chan e) (async/close! res-chan)))))


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
  (cond
    (empty? txi)
    (throw (ex-info (str "Empty or nil transaction item found in transaction.")
                    {:status 400
                     :error  :db/invalid-transaction}))

    (not (map? txi))
    (throw (ex-info (str "All transaction items must be maps/objects, at least one is not.")
                    {:status 400
                     :error  :db/invalid-transaction}))

    :else
    (let [[updated-txi found-txis] (extract-children* txi tx-state)]
      (if found-txis
        ;; recur on children (nested transactions) for possibly additional children
        (let [found-nested (mapcat #(extract-children % tx-state) found-txis)]
          (conj found-nested updated-txi))
        [updated-txi]))))


(defn ->tx-state
  [db block-instant {:keys [auth auth-sid authority authority-sid tx-permissions txid cmd sig nonce type] :as tx-map}]
  (let [tx-type   (keyword type)
        tx        (case tx-type                             ;; command type is either :tx or :new-ledger
                    :tx (:tx tx-map)
                    :new-ledger (tx-util/create-new-ledger-tx tx-map))
        db-before (cond-> db
                          tx-permissions (assoc :permissions tx-permissions))]
    {:db-before        db-before
     :db-root          db
     :db-after         (atom nil)                           ;; store updated db here
     :flakes           nil                                  ;; holds final list of flakes for tx once complete
     :permissions      tx-permissions
     :auth-id          auth                                 ;; auth id string in _auth/id
     :auth             auth-sid                             ;; auth subject-id integer
     :authority-id     authority                            ;; authority id string as per _auth/id (or nil if no authority)
     :authority        authority-sid                        ;; authority subject-id integer (or nil if no authority)
     :t                (dec (:t db))
     :instant          block-instant
     :txid             txid
     :tx-type          tx-type
     :tx               tx
     :tx-string        cmd
     :signature        sig
     :nonce            nonce
     :fuel             (atom {:stack   []
                              :credits 1000000
                              :spent   0})
     ;; hold map of all tempids to their permanent ids. After initial processing will use this to fill
     ;; all tempids with the permanent ids.
     :tempids          (atom {})
     ;; hold same tempids as :tempids above, but stores them in insertion order to ensure when
     ;; assigning permanent ids, it will be done in a predicable order
     :tempids-ordered  (atom [])
     ;; idents (two-tuples of unique predicate + value) may be used multiple times in same tx
     ;; we keep the ones we've already resolved here as a cache
     :idents           (atom {})
     ;; if a tempid resolves to existing subject via :upsert predicate, set it here. tempids don't need
     ;; to check for existing duplicate values, but if a tempid resolves via upsert, we need to check it
     :upserts          (atom #{})                           ;; cache of resolved identities
     ;; Unique predicate + value used in transaction kept here, to ensure the same unique is not used
     ;; multiple times within the transaction
     :uniques          (atom #{})
     ;; If a predicate schema change removes an index (either by turning off index:true or unique:true)
     ;; then we capture the subject ids here and pass back in the transaction result for indexing
     :remove-from-post (atom nil)
     ;; we may generate new tags as part of the transaction. Holds those new tags, but also a cache
     ;; of tag lookups to speed transaction by avoiding full lookups of the same tag multiple times in same tx
     :tags             (atom nil)
     ;; Some predicates may require extra validation after initial processing, we register functions
     ;; here for that purpose, 'cache' holds cached functions that are ready to execute
     :validate-fn      (atom {:queue   (list) :cache {}
                              ;; need to track respective flakes for predicates (for tx-spec) and subject changes (collection-specs)
                              :tx-spec nil :c-spec nil})}))


(defn resolve-temp-flakes
  [temp-flakes multi? {:keys [db-before tempids upserts] :as tx-state}]
  (go-try
    (loop [[^Flake tf & r] temp-flakes
           flakes []]
      (if (nil? tf)
        flakes
        (let [s             (:s tf)
              o             (:o tf)
              ^Flake flake  (cond-> tf
                                    (tempid/TempId? s) (assoc :s (get @tempids s))
                                    (tempid/TempId? o) (assoc :o (get @tempids o)))
              retract-flake (when (contains? @upserts s)    ;; if was an upsert resolved, could be an existing flake that needs to get retracted
                              (first (<? (tx-retract/flake (:s flake) (:p tf) (if multi? (:o flake) nil) tx-state))))
              pred-info     (fn [property] (dbproto/-p-prop db-before property (:p flake)))
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


(defn statements-pipeline
  [tx-state tx]
  (async/go
    (let [queue-ch  (async/to-chan! tx)
          result-ch (async/chan parallelism)
          af        (partial generate-statements tx-state)]

      (async/pipeline-async parallelism result-ch af queue-ch)

      (loop [tx* []]
        (let [next-res (async/<! result-ch)]
          (cond
            ;; no more functions, complete - queue-ch closed as queue was exhausted
            (nil? next-res)
            tx*

            ;; exception, close channels and return exception
            (util/exception? next-res)
            (do (async/close! queue-ch)
                (async/close! result-ch)
                next-res)

            ;; anything else, all good - keep going
            :else (recur (conj tx* next-res))))))))


(defn do-transact
  [tx-state tx]
  (go-try
    (->> tx
         (mapcat #(extract-children % tx-state))
         (statements-pipeline tx-state)
         <?
         (tempid/assign-subject-ids tx-state)
         (finalize-flakes tx-state)
         <?)))


(defn update-db-after
  "Updates db-after into tx-state"
  [db-after tx-state]
  (reset! (:db-after tx-state) db-after)
  db-after)


(defn build-transaction
  [tx-state]
  (go-try
    (let [{:keys [db-root auth-id authority-id txid tx t tx-type fuel permissions]} tx-state
          tx-flakes        (<? (do-transact tx-state tx))
          tx-meta-flakes   (tx-meta/tx-meta-flakes tx-state nil)
          tempids-map      (tempid/result-map tx-state)
          all-flakes       (cond-> (into tx-flakes tx-meta-flakes)
                                   (not-empty tempids-map) (conj (tempid/flake tempids-map t))
                                   @(:tags tx-state) (into (tags/create-flakes tx-state)))

          ;; kick off hash process in the background, it can take a while
          hash-flake       (future (tx-meta/generate-hash-flake all-flakes tx-state))
          fast-forward-db? (:tt-id db-root)
          ;; final db that can be used for any final testing/spec validation
          db-after         (-> (if fast-forward-db?
                                 (<? (dbproto/-forward-time-travel db-root all-flakes))
                                 (<? (dbproto/-with-t db-root all-flakes)))
                               tx-util/make-candidate-db
                               (tx-meta/add-tx-hash-flake @hash-flake)
                               (update-db-after tx-state))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-root [:stats :size]))

          ;; kick off permissions, returns async channel so allow to process in the background
          ;; spec error reporting (next line) will take precedence over permission errors

          ;; runs all 'spec' validations
          spec-errors      (<? (tx-validate/run-queued-specs all-flakes tx-state parallelism))
          perm-errors      (when (and (nil? spec-errors)    ;; only run permissions errors if no spec errors
                                      (not (true? (:root? permissions))))
                             (<? (tx-validate/run-permissions-checks all-flakes tx-state parallelism)))]

      (cond-> {:txid         txid
               :t            t
               :auth         auth-id
               :authority    authority-id
               :db-before    db-root
               :db-after     db-after                       ;; will get replaced if there is an error
               :status       200                            ;; will get replaced if there is an error
               :errors       nil                            ;; will get replaced if there is an error
               :flakes       (conj all-flakes @hash-flake)  ;; will get replaced if there is an error
               :hash         (.-o ^Flake @hash-flake)       ;; will get replaced if there is an error
               :tempids      tempids-map                    ;; will get replaced if there is an error
               :bytes        tx-bytes                       ;; will get replaced if there is an error
               :remove-preds (tx-schema/remove-from-post-result tx-state) ;; will get replaced if there is an error
               :fuel         nil                            ;; additional fuel will be consumed while processing validations, fill in at end
               :type         tx-type}

              ;; replace response with error response if errors detected
              spec-errors
              (-> (tx-error/spec-error spec-errors tx-state) <?)

              ;; only care about permission errors if no spec errors exist
              perm-errors
              (-> (tx-error/spec-error perm-errors tx-state) <?)

              ;; add fuel at end
              true
              (#(assoc % :fuel (+ (:spent @fuel) tx-bytes (count all-flakes) 1)))))))


(defn transact
  [{:keys [db-after instant]} tx-map]
  (async/go
    (try
      (when (not-empty (:deps tx-map))                      ;; transaction has dependencies listed, verify they are satisfied
        (<? (tx-validate/tx-deps-check db-after tx-map)))
      (let [start-time (util/current-time-millis)
            tx-map*    (<? (tx-auth/add-auth-ids-permissions db-after tx-map))
            tx-state   (->tx-state db-after instant tx-map*)
            result     (async/<! (build-transaction tx-state))
            result*    (if (util/exception? result)
                         (<? (tx-error/handler result tx-state))
                         result)]
        (assoc result* :duration (str (- (util/current-time-millis) start-time) "ms")))
      (catch Exception e
        (async/<! (tx-error/pre-processing-handler e db-after tx-map))))))
