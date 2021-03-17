(ns fluree.db.ledger.transact.auth
  (:refer-clojure :exclude [resolve])
  (:require [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util]
            [fluree.db.auth :as auth]
            [fluree.db.permissions :as permissions]))

(defn- valid-authority?
  [db auth authority]
  (go-try
    (if (empty? (<? (dbproto/-search db [auth "_auth/authority" authority])))
      (throw (ex-info (str authority " is not an authority for auth: " auth)
                      {:status 403 :error :db/invalid-auth})) true)))

(defn add-auth-ids-permissions
  "Figures out transaction permissions, returns map."
  [db tx-map]
  (go-try
    (let [{:keys [auth authority]} tx-map
          auth-id-ch      (dbproto/-subid db ["_auth/id" auth] true)
          authority-id-ch (when authority
                            (let [authority-id (if (string? authority) ["_auth/id" authority] authority)]
                              (dbproto/-subid db authority-id true)))
          auth-sid        (async/<! auth-id-ch)
          _               (when (util/exception? auth-sid)
                            (throw (ex-info (str "Auth id for transaction does not exist in the database: " auth)
                                            {:status 403 :error :db/invalid-auth})))
          ;; validate authority is valid or throw
          authority-sid   (when authority
                            (let [authority_id (async/<! authority-id-ch)
                                  _            (when (util/exception? authority_id)
                                                 (throw (ex-info (str "Authority " authority " does not exist.")
                                                                 {:status 403 :error :db/invalid-auth})))]
                              (<? (valid-authority? db auth-sid authority_id))
                              authority_id))
          roles           (<? (auth/roles db auth-sid))
          tx-permissions  (-> (<? (permissions/permission-map db roles :transact))
                              (assoc :auth auth-sid))]
      (assoc tx-map :auth-sid       auth-sid
                    :authority-sid  authority-sid
                    :tx-permissions tx-permissions))))
