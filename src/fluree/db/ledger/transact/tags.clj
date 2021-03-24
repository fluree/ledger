(ns fluree.db.ledger.transact.tags
  (:refer-clojure :exclude [new resolve])
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]))

;; operations related to resolving and creating new tags

(defn- temp-flake->flake
  "Transforms a TempId based flake into a flake."
  [{:keys [tempids t] :as tx-state} temp-flake]
  (flake/->Flake (get @tempids temp-flake) const/$_tag:id (:user-string temp-flake) t true nil))


(defn create-flakes
  "If tags were created via new tempids, we need to create the actual flakes.
  This must happen in the transaction pipeline after tempids have been assigned

  In tx-state we look at the tags map which has keys of formatted tag names (strings) and
  values of either a resolved subject id, or a Tempid if the tag could not be resolved.
  For example, if below the 'yellow' tag already existed, but the 'green' tag did not, it would look like:
  {
   'person/favColor:yellow' 12345678
   'person/favColor:green'  #Tempid{:user-string `person/favColor:green` :collection '_tag' :unique :person/favColor:green}
  }"
  [{:keys [tags] :as tx-state}]
  (->> (vals @tags)
       (filter tempid/TempId?)
       (map (partial temp-flake->flake tx-state))))

(defn create
  "Generates a _tag tempid"
  [tag {:keys [tempids] :as tx-state}]
  (let [tempid (tempid/->TempId "_tag" "_tag" (keyword tag) false)]
    (tempid/register tempid tx-state)
    tempid))


(defn resolve
  "Returns the subject id of the tag if it exists, or a tempid for a new tag."
  [tag pred-info {:keys [tags db-root] :as tx-state}]
  (go-try
    (let [pred-name (or (pred-info :name)
                        (throw (ex-info (str "Trying to resolve predicate name for tag resolution but name is unknown for pred: " (pred-info :id))
                                        {:status 400
                                         :error  :db/invalid-tx
                                         :tags   tag})))
          tag-name  (if (.contains tag "/") tag (str pred-name ":" tag))

          ;; find tag in cache for this transaction, or attempt to resolve in database
          resolved  (or (get @tags tag-name)
                        (when-let [tag-sid (<? (dbproto/-tag-id db-root tag-name))]
                          (swap! tags assoc tag-name tag-sid)
                          tag-sid))]

      (cond
        ;; don't generate new tags for :restrictTag
        (and (pred-info :restrictTag)
             (or (nil? resolved) (tempid/TempId? resolved)))
        (throw (ex-info (str tag " is not a valid tag. The restrictTag property for: " pred-name
                             " is true. Therefore, a tag with the id " pred-name ":" tag " must already exist.")
                        {:status 400
                         :error  :db/invalid-tx
                         :tags   tag}))

        (nil? resolved) (create tag-name tx-state)

        :else resolved))))
