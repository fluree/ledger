(ns fluree.db.ledger.bootstrap
  (:require [clojure.string :as str]
            [fluree.db.flake :as flake]
            [fluree.crypto :as crypto]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.json :as json]
            [fluree.db.constants :as const]
            [fluree.db.session :as session]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.core :as util])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)

(declare bootstrap-txn)


(defn get-block-hash
  "Note this must be in the proper sort order before executing"
  [flakes]
  (->> flakes
       (mapv #(let [^Flake f %] (vector (.-s f) (.-p f) (.-o f) (.-t f) (.-op f) (.-m f))))
       (json/stringify)
       (crypto/sha3-256)))


(defn bootstrap-data->fparts
  [bootstrap-txn]
  (let [collection->id (->> bootstrap-txn
                            (filter #(= "_collection" (-> % :_id first)))
                            (reduce
                              (fn [acc txi]
                                (assoc acc (:name txi) (-> txi :_id second)))
                              {}))
        ident->id      (->> bootstrap-txn
                            (reduce
                              (fn [acc txi]
                                (let [[collection sid] (:_id txi)
                                      cid        (get collection->id collection)
                                      subject-id (flake/->sid cid sid)]
                                  (reduce-kv
                                    (fn [acc2 k v] (assoc acc2 [(str (name collection) "/" (name k)) v] subject-id))
                                    acc (dissoc txi :_id))))
                              {}))
        ;; predicate name to final predicate id.. i.e {"_user/username" 10}
        predicate->id  (->> bootstrap-txn
                            (filter #(= "_predicate" (-> % :_id first)))
                            (reduce
                              (fn [acc txi]
                                (assoc acc (:name txi) (-> txi :_id (second))))
                              {}))
        tag-pred?      (->> bootstrap-txn
                            (filter #(and (= "_predicate" (-> % :_id first))
                                          (= "tag" (:type %))))
                            (map :name)
                            (into #{}))
        ref-pred       (->> bootstrap-txn
                            ;; for now, only things that point to _collection/collection are refs. This logic will have to evolve as needed
                            (filter #(and (= "_predicate" (-> % :_id first))
                                          (or (= "ref" (:type %))
                                              (= "tag" (:type %)))))
                            (map #(-> % :_id second))
                            (into #{}))
        index-pred     (->> bootstrap-txn
                            (filter #(and (= "_predicate" (-> % :_id first))
                                          (or (:index %)
                                              (:unique %))))
                            (map #(-> % :_id second))
                            ;; all ref predicates are indexed as well, so add them in.
                            (into ref-pred))
        fparts         (->> bootstrap-txn
                            (reduce
                              (fn [acc txi]
                                (let [[collection sid] (:_id txi)
                                      cid        (get collection->id collection)
                                      subject-id (flake/->sid cid sid)]
                                  (reduce-kv
                                    (fn [acc2 k v]
                                      (let [p-str (str (name collection) "/" (name k))
                                            p     (get predicate->id p-str)
                                            v     (cond
                                                    (util/pred-ident? v) (get ident->id v)
                                                    (tag-pred? p-str) (get ident->id ["_tag/id" (str p-str ":" v)])
                                                    :else v)]
                                        (if (vector? v)     ;; multi-cardinality values
                                          (reduce #(conj %1 [subject-id p %2]) acc2 v)
                                          (conj acc2 [subject-id p v]))))
                                    acc
                                    (dissoc txi :_id))))
                              []))]
    {:fparts     fparts
     :index-pred index-pred
     :ref-pred   ref-pred
     :pred->id   predicate->id
     :ident->id  ident->id}))


;; TODO - too easy to forget to adjust this if we add a new collection type - we should
;; have an extra check when loading to ensure we have all the ecounts correct.
(def genesis-ecount {const/$_predicate  (flake/->sid const/$_predicate 1000)
                     const/$_collection (flake/->sid const/$_collection 19)
                     const/$_tag        (flake/->sid const/$_tag 1000)
                     const/$_fn         (flake/->sid const/$_fn 1000)
                     const/$_user       (flake/->sid const/$_user 1000)
                     const/$_auth       (flake/->sid const/$_auth 1000)
                     const/$_role       (flake/->sid const/$_role 1000)
                     const/$_rule       (flake/->sid const/$_rule 1000)
                     const/$_setting    (flake/->sid const/$_setting 1000)
                     const/$_shard      (flake/->sid const/$_shard 1000)})



(defn master-auth-flake
  [t pred->id ident->id auth-subid master-authority]
  (let [db-setting-id (flake/->sid const/$_setting 0)
        true-fn-sid   (flake/->sid const/$_fn 0)
        false-fn-sid  (flake/->sid const/$_fn 1)
        rule-sid      (flake/->sid const/$_rule 0)
        role-sid      (flake/->sid const/$_role 0)]
    (when-not master-authority
      (throw (ex-info (str "No Master Authority provided when bootstrapping.")
                      {:status 500 :error :db/unexpected-error})))
    (when-not (get pred->id "_auth/id")
      (throw (ex-info (str "Unable to determine _auth/id predicate id when bootstrapping.")
                      {:status 500 :error :db/unexpected-error})))
    ;(when-not (get pred->id "_setting/id")
    ;  (throw (ex-info (str "Unable to determine _setting/id predicate id when bootstrapping.")
    ;                  {:status 500 :error :db/unexpected-error})))
    ;(when-not (get pred->id "_setting/defaultAuth")
    ;  (throw (ex-info (str "Unable to determine _setting/defaultAuth predicate id when bootstrapping.")
    ;                  {:status 500 :error :db/unexpected-error})))
    [
     ;; add a true predicate function
     (flake/new-flake true-fn-sid (get pred->id "_fn/name") "true" t true)
     (flake/new-flake true-fn-sid (get pred->id "_fn/doc") "Allows access to any rule or spec this is attached to." t true)
     (flake/new-flake true-fn-sid (get pred->id "_fn/code") "true" t true)
     ;; add a false predicate function (just for completeness)
     (flake/new-flake false-fn-sid (get pred->id "_fn/name") "false" t true)
     (flake/new-flake false-fn-sid (get pred->id "_fn/doc") "Denies access to any rule or spec this is attached to." t true)
     (flake/new-flake false-fn-sid (get pred->id "_fn/code") "false" t true)

     ;; add a 'root' rule
     (flake/new-flake rule-sid (get pred->id "_rule/id") "root" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/doc") "Root rule, gives full access" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/collection") "*" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/predicates") "*" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/fns") true-fn-sid t true)
     (flake/new-flake rule-sid (get pred->id "_rule/ops") (get ident->id ["_tag/id" "_rule/ops:all"]) t true)

     ;; add a 'root' role
     (flake/new-flake role-sid (get pred->id "_role/id") "root" t true)
     (flake/new-flake role-sid (get pred->id "_role/doc") "Root role." t true)
     (flake/new-flake role-sid (get pred->id "_role/rules") rule-sid t true)

     ;; add auth record, and assign root rule
     (flake/new-flake auth-subid (get pred->id "_auth/id") master-authority t true)
     (flake/new-flake auth-subid (get pred->id "_auth/roles") role-sid t true)

     ;; add ledger that uses master auth
     (flake/new-flake db-setting-id (get pred->id "_setting/ledgers") auth-subid t true)
     (flake/new-flake db-setting-id (get pred->id "_setting/language") (get ident->id ["_tag/id" "_setting/language:en"]) t true)
     (flake/new-flake db-setting-id (get pred->id "_setting/id") "root" t true)]))

(defn bootstrap-memory-db
  "Bootstraps a blank db fully in-memory.
  Does not attempt to create the db, or index the db to disk. Holds everything in memory.

  Returns a standard 'block' map, but also include the :db key that contains the newly created
  db."
  ([conn ledger]
   (bootstrap-memory-db conn ledger nil))
  ([conn ledger {:keys [master-auth-id master-auth-private txid cmd sig]}]
   (let [blank-db             (session/blank-db conn ledger)
         timestamp            (System/currentTimeMillis)
         {:keys [novelty stats]} blank-db
         master-auth-private* (or master-auth-private (-> (crypto/generate-key-pair) :private))
         master-auth-id*      (or master-auth-id (crypto/account-id-from-private master-auth-private*))
         ;; if a txid isn't provided, we just make one up by hashing the auth-id so something exists for the genesis block
         txid*                (or txid (crypto/sha2-256 master-auth-id*))
         auth-subid           (flake/->sid 6 0)
         block                1
         t                    -1
         block-t              -2
         {:keys [fparts index-pred ref-pred pred->id ident->id]} (bootstrap-data->fparts bootstrap-txn)
         flakes               (reduce (fn [acc [s p o]]
                                        (conj acc (flake/new-flake s p o t true)))
                                      (flake/sorted-set-by flake/cmp-flakes-spot) fparts)
         authority-flakes     (master-auth-flake t pred->id ident->id auth-subid master-auth-id*)
         flakes*              (-> (into flakes authority-flakes)
                                  (conj (flake/new-flake t (get pred->id "_tx/id") txid* t true)
                                        (flake/new-flake t (get pred->id "_tx/nonce") timestamp t true)
                                        (flake/new-flake block-t (get pred->id "_block/number") 1 block-t true)
                                        (flake/new-flake block-t (get pred->id "_block/instant") timestamp block-t true)
                                        (flake/new-flake block-t (get pred->id "_block/transactions") -1 block-t true)
                                        (flake/new-flake block-t (get pred->id "_block/transactions") -2 block-t true)))
         hash                 (get-block-hash flakes*)
         block-flakes         [(flake/new-flake block-t (get pred->id "_block/hash") hash block-t true)
                               (flake/new-flake block-t (get pred->id "_block/ledgers") auth-subid block-t true)]
         flakes+block         (into flakes* block-flakes)

         {:keys [spot psot post opst tspo]} novelty
         db                   (assoc blank-db :block block
                                              :t block-t
                                              :ecount genesis-ecount
                                              :novelty (assoc novelty :spot (into spot flakes+block)
                                                                      :psot (into psot flakes+block)
                                                                      :post (into post (filter #(index-pred (.-p ^Flake %)) flakes+block))
                                                                      :opst (into opst (filter #(ref-pred (.-p ^Flake %)) flakes+block))
                                                                      :tspo (into tspo flakes+block)
                                                                      :size (flake/size-bytes flakes+block))
                                              :stats (assoc stats :flakes (count flakes+block)
                                                                  :size (flake/size-bytes flakes+block)))]
     {:db     db
      :block  block
      :t      block-t
      :flakes flakes+block
      :hash   hash
      :txns   {txid {:t   t
                     :cmd cmd
                     :sig sig}}})))


(defn bootstrap-db
  "Bootstraps a new db from a signed new-db message."
  [{:keys [conn group]} {:keys [cmd sig]}]
  (go-try
   (let [txid          (crypto/sha3-256 cmd)
         new-db-name   (-> cmd json/parse :db)
         [network dbid] (if (sequential? new-db-name)
                          new-db-name
                          (str/split new-db-name #"/"))
         _             (when (or (txproto/ledger-exists? group network dbid)
                                 ;; also check for block 1 on disk as a precaution
                                 (<? (storage/read-block conn network dbid 1)))
                         (throw (ex-info (str "Ledger " network "/$" dbid " already exists! Create unsuccessful.")
                                         {:status 500
                                          :error  :db/unexpected-error})))
         master-authid (crypto/account-id-from-message cmd sig)
         block         (bootstrap-memory-db conn [network dbid] {:master-auth-id master-authid :txid txid :cmd cmd :sig sig})
         new-db        (:db block)
         block-data    (dissoc block :db)
         _             (<? (storage/write-block conn network dbid block-data))
         ;; todo - should create a new command to register new DB that first checks raft
         _             (<? (txproto/register-genesis-block-async (:group conn) network dbid))
                                        ;block-point-success? (async/<! (txproto/propose-new-block-async (:group conn) network dbid block-data))

         {:keys [network dbid block fork] :as indexed-db}
         (<? (indexing/refresh new-db))

         dbgroup       (-> indexed-db :conn :group)
         indexed-block (get-in indexed-db [:stats :indexed])]

     ;; write out new index point
     (<? (txproto/initialized-ledger-async dbgroup txid network dbid block
                                           fork indexed-block))
     indexed-db)))

(def bootstrap-txn
  [{:_id     ["_collection" const/$_predicate]
    :name    "_predicate"
    :doc     "Schema predicate definition"
    :version "1"}
   {:_id     ["_collection" const/$_collection]
    :name    "_collection"
    :doc     "Schema collections list"
    :version "1"}
   {:_id     ["_collection" const/$_tag]
    :name    "_tag"
    :doc     "Tags"
    :version "1"}
   {:_id     ["_collection" const/$_fn]
    :name    "_fn"
    :doc     "Database functions"
    :version "1"}
   {:_id     ["_collection" const/$_user]
    :name    "_user"
    :doc     "Database users"
    :version "1"}
   {:_id     ["_collection" const/$_auth]
    :name    "_auth"
    :doc     "Auth records. Every db interaction is performed by an auth record which governs permissions."
    :version "1"}
   {:_id     ["_collection" const/$_role]
    :name    "_role"
    :doc     "Roles group multiple permission rules to an assignable category, like 'employee', 'customer'."
    :version "1"}
   {:_id     ["_collection" const/$_rule]
    :name    "_rule"
    :doc     "Permission rules"
    :version "1"}
   {:_id     ["_collection" const/$_setting]
    :name    "_setting"
    :doc     "Database settings."
    :version "1"}
   {:_id     ["_collection" const/$_shard]
    :name    "_shard"
    :doc     "Shard settings."
    :version "1"}


   ;; value type tags
   {:_id ["_tag" const/_predicate$type:string]
    :id  "_predicate/type:string"}
   {:_id ["_tag" const/_predicate$type:ref]
    :id  "_predicate/type:ref"}
   {:_id ["_tag" const/_predicate$type:boolean]
    :id  "_predicate/type:boolean"}
   {:_id ["_tag" const/_predicate$type:instant]
    :id  "_predicate/type:instant"}
   {:_id ["_tag" const/_predicate$type:uuid]
    :id  "_predicate/type:uuid"}
   {:_id ["_tag" const/_predicate$type:uri]
    :id  "_predicate/type:uri"}
   {:_id ["_tag" const/_predicate$type:bytes]
    :id  "_predicate/type:bytes"}
   {:_id ["_tag" const/_predicate$type:int]
    :id  "_predicate/type:int"}
   {:_id ["_tag" const/_predicate$type:long]
    :id  "_predicate/type:long"}
   {:_id ["_tag" const/_predicate$type:bigint]
    :id  "_predicate/type:bigint"}
   {:_id ["_tag" const/_predicate$type:float]
    :id  "_predicate/type:float"}
   {:_id ["_tag" const/_predicate$type:double]
    :id  "_predicate/type:double"}
   {:_id ["_tag" const/_predicate$type:bigdec]
    :id  "_predicate/type:bigdec"}
   {:_id ["_tag" const/_predicate$type:tag]
    :id  "_predicate/type:tag"}
   {:_id ["_tag" const/_predicate$type:json]
    :id  "_predicate/type:json"}
   {:_id ["_tag" const/_predicate$type:geojson]
    :id  "_predicate/type:geojson"}

   ;; _rule ops
   {:_id ["_tag" const/_rule$ops:all]
    :id  "_rule/ops:all"}
   {:_id ["_tag" const/_rule$ops:transact]
    :id  "_rule/ops:transact"}
   {:_id ["_tag" const/_rule$ops:query]
    :id  "_rule/ops:query"}
   {:_id ["_tag" const/_rule$ops:logs]
    :id  "_rule/ops:logs"}
   {:_id ["_tag" const/_rule$ops:token]
    :id  "_rule/ops:token"}

   ;; _setting/consensus types
   {:_id ["_tag" const/_setting$consensus:raft]
    :id  "_setting/consensus:raft"}
   {:_id ["_tag" const/_setting$consensus:pbft]
    :id  "_setting/consensus:pbft"}

   ;; _setting/language languages
   {:_id ["_tag" const/_setting$language:ar]
    :id  "_setting/language:ar"
    :doc "Arabic"}
   {:_id ["_tag" const/_setting$language:bn]
    :id  "_setting/language:bn"
    :doc "Bengali"}
   {:_id ["_tag" const/_setting$language:br]
    :id  "_setting/language:br"
    :doc "Brazilian Portuguese"}
   {:_id ["_tag" const/_setting$language:cn]
    :id  "_setting/language:cn"
    :doc "Chinese. FullText search uses the Apache Lucene Smart Chinese Analyzer for Chinese and mixed Chinese-English text, https://lucene.apache.org/core/4_0_0/analyzers-smartcn/org/apache/lucene/analysis/cn/smart/SmartChineseAnalyzer.html"}
   {:_id ["_tag" const/_setting$language:en]
    :id  "_setting/language:en"
    :doc "English"}
   {:_id ["_tag" const/_setting$language:es]
    :id  "_setting/language:es"
    :doc "Spanish"}
   {:_id ["_tag" const/_setting$language:fr]
    :id  "_setting/language:fr"
    :doc "French"}
   {:_id ["_tag" const/_setting$language:hi]
    :id  "_setting/language:hi"
    :doc "Hindi"}
   {:_id ["_tag" const/_setting$language:id]
    :id  "_setting/language:id"
    :doc "Indonesian"}
   {:_id ["_tag" const/_setting$language:ru]
    :id  "_setting/language:ru"
    :doc "Russian"}

   ;; _auth/type types
   {:_id ["_tag" const/_auth$type:secp256k1]
    :id  "_auth/type:secp256k1"}
   {:_id ["_tag" const/_auth$type:password-secp256k1]
    :id  "_auth/type:password-secp256k1"}

   ;; _block predicates
   {:_id  ["_predicate" const/$_block:hash]
    :name "_block/hash"
    :doc  "Merkle root of all _tx/hash in this block."
    :type "string"}
   {:_id  ["_predicate" const/$_block:prevHash]
    :name "_block/prevHash"
    :doc  "Previous block's hash"
    :type "string"}
   {:_id                ["_predicate" const/$_block:transactions]
    :name               "_block/transactions"
    :doc                "Reference to transactions included in this block."
    :type               "ref"
    :multi              true
    :restrictCollection "_tx"}
   {:_id                ["_predicate" const/$_block:ledgers]
    :name               "_block/ledgers"
    :doc                "Reference to ledger auth identities that signed this block. Not included in block hash."
    :type               "ref"
    :multi              true
    :restrictCollection "_auth"}
   {:_id   ["_predicate" const/$_block:instant]
    :name  "_block/instant"
    :doc   "Instant this block was created, per the ledger."
    :type  "instant"
    :index true}
   {:_id    ["_predicate" const/$_block:number]
    :name   "_block/number"
    :doc    "Block number for this block."
    :type   "long"
    :unique true}
   {:_id   ["_predicate" const/$_block:sigs]
    :name  "_block/sigs"
    :doc   "List if ledger signatures that signed this block (signature of _block/hash). Not included in block hash."
    :multi true
    :type  "string"}


   ;; _predicate(s)
   ; _predicate/name
   {:_id    ["_predicate" const/$_predicate:name]
    :name   "_predicate/name"
    :doc    "Predicate name."
    :type   "string"
    :unique true}
   ; _predicate/doc
   {:_id  ["_predicate" const/$_predicate:doc]
    :name "_predicate/doc"
    :doc  "Optional docstring for predicate."
    :type "string"}
   ; _predicate/type
   {:_id         ["_predicate" const/$_predicate:type]
    :name        "_predicate/type"
    :doc         "The specific type for this predicate has to be a valueType."
    :type        "tag"
    :restrictTag true}
   ; _predicate/unique
   {:_id  ["_predicate" const/$_predicate:unique]
    :name "_predicate/unique"
    :doc  "If uniqueness for this predicate should be enforced. Unique predicates can be used as an identity."
    :type "boolean"}
   ; _predicate/multi
   {:_id  ["_predicate" const/$_predicate:multi]
    :name "_predicate/multi"
    :doc  "If this predicate supports multiple cardinality, or many values."
    :type "boolean"}
   ; _predicate/index
   {:_id  ["_predicate" const/$_predicate:index]
    :name "_predicate/index"
    :doc  "If this predicate should be indexed."
    :type "boolean"}
   ; _predicate/upsert
   {:_id  ["_predicate" const/$_predicate:upsert]
    :name "_predicate/upsert"
    :doc  "Only valid for unique predicates. When adding a new subject, will upsert existing subject instead of throwing an exception if the value already exists."
    :type "boolean"}
   ; _predicate/component
   {:_id  ["_predicate" const/$_predicate:component]
    :name "_predicate/component"
    :doc  "If the sub-entities for this predicate should always be deleted if this predicate is deleted. Only applies for predicates that refer to another collection."
    :type "boolean"}
   ; _predicate/noHistory
   {:_id  ["_predicate" const/$_predicate:noHistory]
    :name "_predicate/noHistory"
    :doc  "Does not retain any history, making historical queries always use the current value."
    :type "boolean"}
   {:_id  ["_predicate" const/$_predicate:restrictCollection]
    :name "_predicate/restrictCollection"
    :doc  "When an predicate is a reference type (ref), it can be optionally restricted to this collection."
    :type "string"}
   {:_id                ["_predicate" const/$_predicate:spec]
    :name               "_predicate/spec"
    :doc                "Spec performed on this predicate. Specs are run post-transaction, before a new block is committed."
    :type               "ref"
    :restrictCollection "_fn"
    :multi              true}
   {:_id  ["_predicate" const/$_predicate:encrypted]
    :name "_predicate/encrypted"
    :doc  "Boolean flag if this predicate is stored encrypted. Transactions will ignore the _predicate/type and ensure it is a string. Query engines should have the decryption key."
    :type "boolean"}
   {:_id  ["_predicate" const/$_predicate:deprecated]
    :name "_predicate/deprecated"
    :doc  "Boolean flag if this predicate has been deprecated. This is primarily informational, however a warning may be issued with query responses."
    :type "boolean"}
   {:_id  ["_predicate" const/$_predicate:specDoc]
    :name "_predicate/specDoc"
    :doc  "Optional docstring for _predicate/spec."
    :type "string"}
   {:_id                ["_predicate" const/$_predicate:txSpec]
    :name               "_predicate/txSpec"
    :doc                "Spec performed on all of this predicate in a txn. Specs are run post-transaction, before a new block is committed."
    :type               "ref"
    :restrictCollection "_fn"
    :multi              true}
   {:_id  ["_predicate" const/$_predicate:txSpecDoc]
    :name "_predicate/txSpecDoc"
    :doc  "Optional docstring for _predicate/spec."
    :type "string"}
   {:_id  ["_predicate" const/$_predicate:restrictTag]
    :name "_predicate/restrictTag"
    :doc  "If true, a tag, which corresponds to the predicate object must exist before adding predicate-object pair."
    :type "boolean"}
   {:_id  ["_predicate" const/$_predicate:fullText]
    :name "_predicate/fullText"
    :doc  "If true, full text search is enabled on this predicate."
    :type "boolean"}
   {:_id  ["_predicate" const/$_predicate:retractDuplicates]
    :name "_predicate/retractDuplicates"
    :doc  "If false (default), when a transaction creates a new flake that already exists it does not update the existing data. When true, it will always force a retraction/insertion."
    :type "boolean"}



   ;; tag records
   {:_id    ["_predicate" const/$_tag:id]
    :name   "_tag/id"
    :doc    "Namespaced tag id"
    :type   "string"
    :upsert true
    :unique true}
   {:_id  ["_predicate" const/$_tag:doc]
    :name "_tag/doc"
    :doc  "Optional docstring for tag."
    :type "string"}



   ;; _collection predicates
   ; _collection/name
   {:_id    ["_predicate" const/$_collection:name]
    :name   "_collection/name"
    :doc    "Schema collection name"
    :type   "string"
    :unique true}
   {:_id  ["_predicate" const/$_collection:partition]
    :name "_collection/partition"
    :doc  "Partition integer used for new items in this collection, max of 524,287. If not included it inherits from parent, or uses collection's internal counter if no parent."
    :type "int"}
   {:_id  ["_predicate" const/$_collection:doc]
    :name "_collection/doc"
    :doc  "Optional docstring for collection."
    :type "string"}
   ; _collection/version
   {:_id   ["_predicate" const/$_collection:version]
    :name  "_collection/version"
    :doc   "Version number for this collection's schema."
    :type  "string"
    :index true}
   ;; _collection/spec
   {:_id                ["_predicate" const/$_collection:spec]
    :name               "_collection/spec"
    :doc                "Spec for the collection. All entities within this collection must meet this spec. Spec is run post-transaction, but before committing a new block."
    :type               "ref"
    :multi              true
    :restrictCollection "_fn"}

   ;; _collection/specDoc
   {:_id  ["_predicate" const/$_collection:specDoc]
    :name "_collection/specDoc"
    :doc  "Optional docstring for _collection/spec."
    :type "string"}
   ; _collection/shard
   {:_id                ["_predicate" const/$_collection:shard]
    :name               "_collection/shard"
    :doc                "The shard that this collection is assigned to. If none assigned, defaults to 'default' shard."
    :type               "ref"
    :restrictCollection "_shard"}




   ;; _user predicates
   {:_id    ["_predicate" const/$_user:username]
    :name   "_user/username"
    :doc    "Unique account ID (string). Emails are nice for business apps."
    :type   "string"
    :unique true}
   {:_id                ["_predicate" const/$_user:auth]
    :name               "_user/auth"
    :doc                "User's auth records"
    :multi              true
    :unique             true
    :type               "ref"
    :restrictCollection "_auth"}
   {:_id                ["_predicate" const/$_user:roles]
    :name               "_user/roles"
    :doc                "Default roles to use for this user. If roles are specified via an auth record, those will over-ride these roles."
    :type               "ref"
    :multi              true
    :restrictCollection "_role"}
   {:_id  ["_predicate" const/$_user:doc]
    :name "_user/doc"
    :doc  "Optional docstring for user."
    :type "string"}


   ;; _auth predicates
   {:_id    ["_predicate" const/$_auth:id]
    :name   "_auth/id"
    :doc    "Unique auth id. Used to store derived public key (but doesn't have to)."
    :type   "string"
    :unique true}
   {:_id   ["_predicate" const/$_auth:password]
    :name  "_auth/password"
    :doc   "Encrypted password."
    :type  "string"
    :index true}
   {:_id  ["_predicate" const/$_auth:salt]
    :name "_auth/salt"
    :doc  "Salt used for auth record, if the auth type requires it."
    :type "bytes"}
   {:_id                ["_predicate" const/$_auth:roles]
    :name               "_auth/roles"
    :doc                "Reference to roles that this authentication record is governed by."
    :type               "ref"
    :multi              true
    :restrictCollection "_role"}
   {:_id  ["_predicate" const/$_auth:doc]
    :name "_auth/doc"
    :doc  "Optional docstring for auth record."
    :type "string"}
   {:_id                ["_predicate" const/$_auth:type]
    :name               "_auth/type"
    :doc                "Tag to identify underlying auth record type, if necessary."
    :type               "tag"
    :restrictCollection "_auth"
    :restrictTag        true}
   {:_id                ["_predicate" const/$_auth:authority]
    :name               "_auth/authority"
    :doc                "Authorities for this auth record. References another _auth record."
    :type               "ref"
    :multi              true
    :restrictCollection "_auth"}
   {:_id   ["_predicate" const/$_auth:fuel]
    :name  "_auth/fuel"
    :doc   "Fuel this auth record has."
    :type  "long"
    :index true}


   ;; _role predicates
   {:_id    ["_predicate" const/$_role:id]
    :name   "_role/id"
    :doc    "Unique role id. A role contains a collection of rule permissions. This role id can be used to easily get a set of permission for a role like 'customer', 'employee', etc."
    :type   "string"
    :unique true}
   {:_id  ["_predicate" const/$_role:doc]
    :name "_role/doc"
    :doc  "Optional docstring for role."
    :type "string"}
   {:_id                ["_predicate" const/$_role:rules]
    :name               "_role/rules"
    :doc                "Reference to rules this role contains. Multi-cardinality. Rules define actual permissions."
    :type               "ref"
    :multi              true
    :restrictCollection "_rule"}


   ;; _rule predicates
   {:_id    ["_predicate" const/$_rule:id]
    :name   "_rule/id"
    :doc    "Optional rule unique id"
    :type   "string"
    :unique true}
   {:_id  ["_predicate" const/$_rule:doc]
    :name "_rule/doc"
    :doc  "Optional docstring for rule."
    :type "string"}
   {:_id   ["_predicate" const/$_rule:collection]
    :name  "_rule/collection"
    :doc   "Stream name/glob that should match."
    :type  "string"
    :index true}
   {:_id   ["_predicate" const/$_rule:predicates]
    :name  "_rule/predicates"
    :doc   "Specific predicate this rule applies to, or wildcard '*' predicate which will be run only if no specific predicate rules match."
    :type  "string"
    :index true
    :multi true}
   {:_id                ["_predicate" const/$_rule:fns]
    :name               "_rule/fns"
    :doc                "Ref to functions, which resolve to true or false."
    :type               "ref"
    :multi              true
    :restrictCollection "_fn"}

   {:_id         ["_predicate" const/$_rule:ops]
    :name        "_rule/ops"
    :doc         "Operations (using tags) that this rule applies to."
    :multi       true
    :type        "tag"
    :restrictTag true}
   {:_id   ["_predicate" const/$_rule:collectionDefault]
    :name  "_rule/collectionDefault"
    :doc   "Default rule applies to collection only if no other more specific rule matches."
    :type  "boolean"
    :index true}
   {:_id  ["_predicate" const/$_rule:errorMessage]
    :name "_rule/errorMessage"
    :doc  "The error message that should be displayed if this rule makes a transaction fail."
    :type "string"}


   ;; _fn predicates
   {:_id    ["_predicate" const/$_fn:name]
    :name   "_fn/name"
    :doc    "Function name"
    :type   "string"
    :unique true}
   {:_id  ["_predicate" const/$_fn:params]
    :name "_fn/params"
    :doc  "List of parameters this function supports."
    :type "string"}
   {:_id  ["_predicate" const/$_fn:code]
    :name "_fn/code"
    :doc  "Actual database function code."
    :type "string"}
   {:_id  ["_predicate" const/$_fn:doc]
    :name "_fn/doc"
    :doc  "Doc string describing this function."
    :type "string"}
   {:_id  ["_predicate" const/$_fn:spec]
    :name "_fn/spec"
    :doc  "Optional spec for parameters. Spec should be structured as a map, parameter names are keys and the respective spec is the value."
    :type "json"}
   {:_id  ["_predicate" const/$_fn:language]
    :name "_fn/language"
    :doc  "Programming language used."
    :type "tag"}

   ;; _tx predicates
   {:_id    ["_predicate" const/$_tx:id]
    :name   "_tx/id"
    :doc    "Unique transaction ID."
    :type   "string"
    :unique true}
   {:_id                ["_predicate" const/$_tx:auth]
    :name               "_tx/auth"
    :doc                "Reference to the auth id for this transaction."
    :type               "ref"
    :restrictCollection "_auth"}
   {:_id                ["_predicate" const/$_tx:authority]
    :name               "_tx/authority"
    :doc                "If this transaction utilized an authority, reference to it."
    :type               "ref"
    :restrictCollection "_auth"}
   {:_id   ["_predicate" const/$_tx:nonce]
    :name  "_tx/nonce"
    :doc   "A nonce that helps ensure identical transactions have unique txids, and also can be used for logic within smart functions. Note this nonce does not enforce uniqueness, use _tx/altId if uniqueness must be enforced."
    :type  "long"
    :index true}
   {:_id    ["_predicate" const/$_tx:altId]
    :name   "_tx/altId"
    :doc    "Alternative Unique ID for the transaction that the user can supply. Transaction will throw if not unique."
    :type   "string"
    :unique true}
   {:_id  ["_predicate" const/$_tx:doc]
    :name "_tx/doc"
    :doc  "Optional docstring for the transaction."
    :type "string"}
   {:_id  ["_predicate" const/$_tx:tx]
    :name "_tx/tx"
    :doc  "Original JSON transaction command."
    :type "string"}
   {:_id  ["_predicate" const/$_tx:sig]
    :name "_tx/sig"
    :doc  "Signature of original JSON transaction command."
    :type "string"}
   {:_id  ["_predicate" const/$_tx:tempids]
    :name "_tx/tempids"
    :doc  "Tempid JSON map for this transaction."
    :type "string"}
   {:_id  ["_predicate" const/$_tx:error]
    :name "_tx/error"
    :doc  "Error type and message, if an error happened for this transaction."
    :type "string"}
   {:_id  ["_predicate" const/$_tx:hash]
    :name "_tx/hash"
    :doc  "Error type and message, if an error happened for this transaction."
    :type "string"}

   ;; _setting predicates
   {:_id                ["_predicate" const/$_setting:anonymous]
    :name               "_setting/anonymous"
    :doc                "Reference to auth identity to use for anonymous requests to this db."
    :type               "ref"
    :restrictCollection "_auth"}
   {:_id                ["_predicate" const/$_setting:ledgers]
    :name               "_setting/ledgers"
    :doc                "Reference to auth identities that are allowed to act as ledgers for this database."
    :multi              true
    :type               "ref"
    :restrictCollection "_auth"}
   {:_id         ["_predicate" const/$_setting:consensus]
    :name        "_setting/consensus"
    :doc         "Consensus type for this db."
    :type        "tag"
    :restrictTag true}
   {:_id  ["_predicate" const/$_setting:doc]
    :name "_setting/doc"
    :doc  "Optional docstring for the db."
    :type "string"}
   {:_id  ["_predicate" const/$_setting:passwords]
    :name "_setting/passwords"
    :doc  "Whether password-based authentication is enabled on this db."
    :type "boolean"}
   {:_id  ["_predicate" const/$_setting:txMax]
    :name "_setting/txMax"
    :doc  "Maximum transaction size in bytes."
    :type "long"}
   {:_id    ["_predicate" const/$_setting:id]
    :name   "_setting/id"
    :doc    "Unique setting id."
    :type   "string"
    :unique true}
   {:_id         ["_predicate" const/$_setting:language]
    :name        "_setting/language"
    :doc         "Default database language. Used for full-text search. See docs for valid options."
    :type        "tag"
    :restrictTag true}

   ; _shard
   {:_id    ["_predicate" const/$_shard:name]
    :name   "_shard/name"
    :doc    "Name of this shard"
    :type   "string"
    :unique true}
   {:_id                ["_predicate" const/$_shard:miners]
    :name               "_shard/miners"
    :doc                "Miners (auth records) assigned to this shard"
    :type               "ref"
    :restrictCollection "_auth"
    :multi              true}
   {:_id  ["_predicate" const/$_shard:mutable]
    :name "_shard/mutable"
    :doc  "Whether this shard is mutable. If not specified, defaults to 'false', meaning the data is immutable."
    :type "boolean"}])
