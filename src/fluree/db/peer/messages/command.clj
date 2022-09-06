(ns fluree.db.peer.messages.command
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]))

(set! *warn-on-reflection* true)

(def max-size 10000000)

(def always?
  (constantly true))

(defn small?
  [cmd]
  (-> cmd
      count
      (<= max-size)))

(defn no-colon?
  [s]
  (not (str/includes? s ":")))

(defn network?
  [s]
  (some? (re-matches #"^[a-z0-9-]+$" s)))

(defn ledger-id?
  [s]
  (some? (re-matches #"^[a-z0-9-]+$" s)))

(defn ledger-string?
  [s]
  (some? (re-matches #"^[a-z0-9-]+/[a-z0-9-]+$" s)))

(s/def ::cmd (s/and string? small?))
(s/def ::sig string?)
(s/def ::signed (s/nilable string?))

(s/def ::signed-cmd
  (s/keys :req-un [::cmd ::sig]
          :opt-un [::signed]))

(s/def ::type keyword?)

(s/def ::tx (s/or :map  map?
                  :coll (s/coll-of map?)))
(s/def ::deps (s/coll-of string?))
(s/def ::expire pos-int?)
(s/def ::nonce int?)
(s/def ::network (s/and string? network?))
(s/def ::ledger-id (s/and string? ledger-id?))
(s/def ::ledger (s/or :pair   (s/tuple ::network ::ledger-id)
                      :string (s/and string? ledger-string?)))
(s/def ::snapshot always?)
(s/def ::owners (s/coll-of string?))

(defmulti cmd-data-type :type)

(defmethod cmd-data-type :tx
  [_]
  (s/keys :req-un [::type ::tx ::ledger]
          :opt-un [::deps ::expire ::nonce]))

(defmethod cmd-data-type :new-ledger
  [_]
  (s/keys :req-un [::type ::ledger]
          :opt-un [::auth ::owners ::snapshot ::expire ::nonce]))

(defmethod cmd-data-type :default
  [_]
  (s/keys :req-un [::type]))

(s/def ::cmd-data
  (s/multi-spec cmd-data-type :type))

(defn throw-invalid
  [message]
  (throw (ex-info message
                  {:status 400
                   :error  :db/invalid-command})))

(defn parse-signed-command
  [msg]
  (let [signed-cmd (s/conform ::signed-cmd msg)]
    (when (s/invalid? signed-cmd)
      (throw-invalid (s/explain-str ::signed-cmd msg)))
    signed-cmd))

(defn parse-json
  [cmd]
  (try
    (-> cmd
        json/parse
        (update :type keyword))
    (catch Exception _
      (throw-invalid "Invalid command serialization, could not decode JSON."))))

(defn parse-cmd-data
  [cmd]
  (let [parsed-cmd (parse-json cmd)
        cmd-data   (s/conform ::cmd-data parsed-cmd)]
    (when (s/invalid? cmd-data)
      (throw-invalid (s/explain-str ::cmd-data parsed-cmd)))
    (s/unform ::cmd-data cmd-data)))

(defn parse-auth-id
  [{:keys [cmd sig signed] :as _parsed-command}]
  (try
    (-> signed
        (or cmd)
        (crypto/account-id-from-message sig))
    (catch Exception _
      (throw-invalid "Invalid signature on command."))))

(defn parse-id
  [cmd-str]
  (crypto/sha3-256 cmd-str))

(defn parse
  [msg]
  (let [{:keys [cmd sig signed] :as signed-cmd}
        (parse-signed-command msg)

        id       (parse-id cmd)
        auth-id  (parse-auth-id signed-cmd)
        cmd-data (parse-cmd-data cmd)]
    {:id         id
     :auth-id    auth-id
     :signed-cmd signed-cmd
     :cmd-data   cmd-data}))
