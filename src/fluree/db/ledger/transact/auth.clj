(ns fluree.db.ledger.transact.auth
  (:refer-clojure :exclude [resolve])
  (:require [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util]
            [fluree.db.auth :as auth]
            [fluree.db.permissions :as permissions]
            [fluree.db.util.log :as log]))

(defn- valid-authority?
  [db auth authority]
  (async/go
    (let [connection? (async/<! (dbproto/-search db [auth "_auth/authority" authority]))]
      (cond (util/exception? connection?)
            (do (log/error connection? "valid-authority? search for connection between " [auth "_auth/authority" authority]
                           "unexpectedly failed!")
                false)

            (empty? connection?)
            false

            :else
            true))))


(defn- resolve-auth+authority-sids
  "Returns two-tuple of [auth-sid and authority-sid].
  If no authority exists, returns nil for authority-sid.

  Performs lookups in parallel.

  Returns exception if both auth and authority (when applicable)
  do not resolve to an _auth/id"
  [db auth authority]
  (async/go
    (let [auth-id-ch    (dbproto/-subid db ["_auth/id" auth] true)
          ;; kick off authority check in parallel (when applicable)
          authority-sid (when authority
                          (let [authority-id (if (string? authority) ["_auth/id" authority] authority)]
                            (async/<! (dbproto/-subid db authority-id true))))
          auth-sid      (async/<! auth-id-ch)]
      (cond
        (util/exception? auth-sid)
        (ex-info (str "Auth id for transaction does not exist in the database: " auth)
                 {:status 403 :error :db/invalid-auth})

        (util/exception? authority-sid)
        (ex-info (str "Authority " authority " does not exist.")
                 {:status 403 :error :db/invalid-auth})

        (and authority-sid (false? (async/<! (valid-authority? db auth-sid authority-sid))))
        (ex-info (str authority " is not an authority for auth: " auth)
                 {:status 403 :error :db/invalid-auth})

        :else
        [auth-sid authority-sid]))))


(defn add-auth-ids-permissions
  "Figures out transaction permissions, returns map."
  [db tx-map]
  (go-try
    (let [{:keys [auth authority]} tx-map
          [auth-sid authority-sid] (<? (resolve-auth+authority-sids db auth authority))
          roles          (<? (auth/roles db auth-sid))
          tx-permissions (-> (<? (permissions/permission-map db roles :transact))
                             (assoc :auth auth-sid))]
      (assoc tx-map :auth-sid auth-sid
                    :authority-sid authority-sid
                    :tx-permissions tx-permissions))))
