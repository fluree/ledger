(ns fluree.db.peer.messages.command
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.json :as json]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]))

(set! *warn-on-reflection* true)

(def max-size 10000000)

(defn small?
  [cmd]
  (-> cmd
      count
      (<= max-size)))

(defn no-colon?
  [s]
  (not (str/includes? s ":")))

(s/def ::cmd (s/and string? small?))
(s/def ::sig string?)
(s/def ::signed (s/nilable string?))

(s/def ::signed-command
  (s/keys :req-un [::cmd ::sig]
          :opt-un [::signed]))

(s/def ::type (s/or :string  (s/and string? no-colon?)
                    :symbol  symbol?
                    :keyword keyword?))
(s/def ::data
  (s/keys :req-un [::type]))

(defn throw-invalid
  [message]
  (throw (ex-info message
                  {:status 400
                   :error  :db/invalid-command})))

(defn parse-signed-command
  [msg]
  (let [signed-cmd (s/conform ::signed-command msg)]
    (when (s/invalid? signed-cmd)
      (throw-invalid (s/explain-str ::signed-command msg)))
    signed-cmd))

(defn parse-json
  [cmd]
  (try
    (json/parse cmd)
    (catch Exception _
      (throw-invalid "Invalid command serialization, could not decode JSON."))))

(defn parse-data
  [parsed-cmd]
  (let [data (s/conform ::data parsed-cmd)]
    (when (s/invalid? data)
      (throw-invalid (s/explain-str ::data parsed-cmd)))
    (update data :type (comp keyword second))))

(defn parse-auth-id
  [{:keys [cmd sig signed] :as _parsed-command}]
  (try
    (crypto/account-id-from-message (or signed cmd) sig)
    (catch Exception _
      (throw-invalid "Invalid signature on command."))))

(defn parse-id
  [cmd-str]
  (crypto/sha3-256 cmd-str))

(defn parse
  [msg]
  (let [{:keys [cmd sig signed] :as signed-cmd}
        (parse-signed-command msg)

        parsed-cmd (parse-json cmd)
        cmd-data   (parse-data parsed-cmd)
        auth-id    (parse-auth-id signed-cmd)
        id         (parse-id cmd)]
    {:id      id
     :auth-id auth-id
     :data    cmd-data}))
