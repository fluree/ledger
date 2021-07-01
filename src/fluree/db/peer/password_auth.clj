(ns fluree.db.peer.password-auth
  (:require [fluree.crypto :as crypto]
            [fluree.crypto.hmac :refer [hmac-sha256]]
            [fluree.crypto.scrypt :as scrypt]
            [fluree.crypto.secp256k1 :as secp256k1]
            [alphabase.core :as alphabase]
            [fluree.db.util.core :as util]
            [fluree.db.api :as fdb]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.token-auth :as token-auth]))


(defn password-enabled?
  "Returns true if Fluree password auth feature is enabled"
  [conn]
  (boolean (-> conn :meta :password-auth)))


(defn key-pair-from-password
  "Returns a map with private key, public key, auth-id and salt all as hex values.
  Must provide at least a password (if generating a new record), or include a salt if verifying
  an existing record.

  Inputs are:
  - password - (required plain text) will be string normalized
  - salt     - (optional byte array) used as salt for scrypt, ideally 128+ bits.
                Defaults to random 128 bits and returned in the result map.
  - secret   - (optional byte array) defaults to nil - from server/edge server config, if avail.
               Secret must be identical for all participating servers. Does an HMAC sha of password
               using provided secret, if it exists.

   Outputs a map with keys:
   - public  - public key
   - private - private key
   - id      - Fluree auth-id
   - salt    - salt used to derive private key"
  ([secret password] (key-pair-from-password secret password nil))
  ([secret password salt]
   (let [salt'   (or salt (crypto/random-bytes 16))
         pw'     (-> (crypto/normalize-string password)
                     crypto/string->byte-array)
         sha     (hmac-sha256 pw' secret)
         private (as-> (scrypt/encrypt sha salt') pv
                       (BigInteger. ^bytes pv)
                       (if (neg? pv)
                         (biginteger (* -1 pv))
                         pv)
                       (if (> pv secp256k1/modulus)
                         (biginteger (/ pv 2))              ;; for now rule is divide by 2 if outside curve modulus
                         pv))
         keypair (crypto/generate-key-pair private)]
     (assoc keypair :id (crypto/account-id-from-public (:public keypair))
                    :salt (alphabase/bytes->hex salt')))))


(defn new-key-pair
  "Will generate a new key pair and return map with private key, public key, salt and auth-id.
  If optional secret is provided (byte-array), will utilize hmac sha2 encoding of password
  instead of standard sha2."
  [secret password]
  (key-pair-from-password secret password nil))


(defn verify-identity
  "Will verify password + salt against one or more auth-ids.
  If an auth-id matches, returns a map with hex values of private key, public key, auth-id and salt.
  If no auth-ids match, will return nil.

  Secret is optional, but must be a byte-array if provided.
  Salt is assumed to be a hex string or byte-array.
  Password is plain text."
  [secret password auth-id salt]
  (let [salt'   (if (string? salt)
                  (alphabase/hex->bytes salt)
                  salt)
        keypair (key-pair-from-password secret password salt')]
    (when (= auth-id (:id keypair))
      keypair)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; --- Fluree-specific JWT handling ---
;; The following are fluree-specific JWT encodings
;; and database queries/transactions
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn fluree-create-jwt
  "Creates a Fluree JWT token.
  options are a map with the following optional keys:
  - expire  - expiration time in milliseconds from now
  - payload - map of any desired additional data to include in JWT.
              Gets merged with standard token items listed below.
  - iv      - initialization vector used to AES encrypt password within JWT.
              Default value is the first 16 bytes of the salt

  First verifies password provided (exception if invalid) and derives private key.

  Token includes
  - iss - (:issuer options), 'fluree' by default, should be full name of ledger
  - sub - auth-id that matches a valid password authentication (subject)
  - exp - (:expire options), or 5 minute default in milliseconds
  - iat - issue time, in epoch ms
  - pri - AES encrypted private key that uses (:iv options) as the initialization
          vector, of the first 16 bytes of the salt by default, and the secret
          for the encryption key.

  Note that any validating server must know both the secret and the initialization vector
  to decode the AES encrypted private key, which is assumed to be the first 16 bytes of the
  salt from the auth record."
  [jwt-options ledger password auth-ids+salts options]
  (let [{:keys [secret jwt-secret max-exp]} jwt-options
        auth-ids+salts* (cond
                          (not (sequential? auth-ids+salts))
                          (throw (ex-info (str "Password JWT must take two-tuples of"
                                               "[auth-id salt], provided: " auth-ids+salts)
                                          {:status 401
                                           :error  :db/invalid-auth}))
                          ;; should already be a list of two-tuples
                          (sequential? (first auth-ids+salts))
                          auth-ids+salts

                          ;; make a list of two-tuples
                          :else
                          [auth-ids+salts])
        key-map         (or (some #(verify-identity secret password (first %) (second %)) auth-ids+salts*)
                            (throw (ex-info "Invalid password." {:status 401 :error :db/invalid-password})))
        now             (util/current-time-millis)
        {:keys [expire payload iv]
         :or   {expire max-exp
                iv     (take 16 (alphabase/hex->bytes (:salt key-map)))}} options
        _               (when-not (pos-int? expire)
                          (throw (ex-info "Expire (exp) must be number for a valid JWT and must be greater than the current time." {:status 400 :error :db/invalid-jwt})))
        _               (when (> expire max-exp)
                          (throw (ex-info (str "Requested expire time for token exceeds max expiration configuration allows.")
                                          {:status 400
                                           :error  :db/invalid-request})))

        _               (when-not (string? ledger)
                          (throw (ex-info "Issuer (iss) must be string for a valid JWT and should be ledger name." {:status 400 :error :db/invalid-jwt})))
        enc-pri         (-> (:private key-map)
                            (crypto/aes-encrypt iv secret))
        payload'        (merge payload
                               {:iss ledger                 ;; issuer
                                :sub (:id key-map)          ;; subject
                                :exp (+ now expire)         ;; expires
                                :iat now                    ;; issued at
                                :pri enc-pri})]
    (token-auth/generate-jwt (or jwt-secret secret) payload')))


(defn fluree-renew-jwt
  "Renew a Fluree JWT token.
  options are a map with the following optional keys:
  - expire - expiration time in milliseconds from now
  - iv     - initialization vector used to AES encrypt password within JWT.
             Default value is the first 16 bytes of the salt

  First verifies password provided (exception if invalid) and derives private key.

  Token includes
  - iss  - (:issuer options), 'fluree' by default, should be full name of ledger
  - sub  - auth-id that matches a valid password authentiation (subject)
  - exp  - (:expire options), or 5 minute default in milliseconds
  - iat  - issue time, in epoch ms
  - oiat - original issue time, in epoch ms
  - pri  - AES encrypted private key that uses (:iv options) as the initialization
           vector, of the first 16 bytes of the salt by default, and the secret
           for the encryption key.

  Note that any validating server must know both the secret and the initialization vector
  to decode the AES encrypted private key, which is assumed to be the first 16 bytes of the
  salt from the auth record."
  [jwt-options existing-jwt-map options]
  (let [{:keys [secret jwt-secret max-exp max-renewal]} jwt-options
        original-iat  (or (:oiat existing-jwt-map) (:iat existing-jwt-map))
        now           (util/current-time-millis)
        expire        (or (:expire options) max-exp)
        _             (when-not (pos-int? expire)
                        (throw (ex-info "Expire (exp) must be number for a valid JWT and must be greater than the current time." {:status 400 :error :db/invalid-jwt})))
        _             (when (> expire max-exp)
                        (throw (ex-info (str "Requested expire time for token exceeds max expiration configuration allows.")
                                        {:status 400
                                         :error  :db/invalid-request})))
        max-exp-epoch (+ original-iat max-renewal)
        exp           (min (+ now expire) max-exp-epoch)
        _             (when (> exp max-exp-epoch)
                        (throw (ex-info (str "Requested expire time for token exceeds maximum allowed JWT token renewal time. Please re-login.")
                                        {:status 401
                                         :error  :db/expired-token})))

        payload       (assoc existing-jwt-map :exp exp
                                              :oiat original-iat)]
    (token-auth/generate-jwt (or jwt-secret secret) payload)))


;; TODO - below assumes a username is provided, need to also support a user subject
;; id or any other identifier... need a way to pass in a variable.
(defn- get-user-auth-ids
  "Returns two-tuples of [auth-id salt] associated with user to verify against password auth."
  [conn ledger user-ident]
  (fdb/query-async
    (fdb/db conn ledger)
    {:select ["?auth-ids" "?salt"]
     :where  [["?id" "_user/username" user-ident]
              ["?id" "_user/auth" "?auth"]
              ["?auth" "_auth/salt" "?salt"]
              ["?auth" "_auth/id" "?auth-ids"]]}))


(defn- salt-from-auth-id
  "Returns a salt from an auth-id"
  [conn ledger auth-id]
  (fdb/query-async
    (fdb/db conn ledger)
    {:selectOne "?salt"
     :where     [["?id" "_auth/id" auth-id]
                 ["?id" "_auth/salt" "?salt"]]}))


(defn fluree-decode-jwt
  "Extracts a private key from a signed JWT.

  Returns a core async channel that will eventually contain a valid result or exception."
  [conn jwt]
  (go-try
    (let [{:keys [secret]} (-> conn :meta :password-auth)
          payload (token-auth/verify-jwt secret jwt)
          {:keys [iss sub pri]} payload                     ;; iss is ledger, sub is auth-id
          salt    (<? (salt-from-auth-id conn iss sub))
          _       (when-not (string? salt)
                    (throw (ex-info (str "Unable to retrieve salt for auth id: " sub ".")
                                    {:status 401
                                     :error  :db/invalid-auth})))
          iv      (->> salt
                       (alphabase/hex->bytes)
                       (take 16))
          private (crypto/aes-decrypt pri iv secret :string :hex)]
      private)))


(defn fluree-auth-map
  "Returns an auth map for a JWT token. Throws an exception if token is invalid, or if
  ledger, if provided, is not the same ledger that is encoded in the token as the issuer (iss)."
  ([conn jwt] (fluree-auth-map conn nil jwt))
  ([conn ledger jwt]
   (let [{:keys [secret]} (-> conn :meta :password-auth)
         payload (token-auth/verify-jwt secret jwt)
         {:keys [iss sub pri]} payload                      ;; iss is ledger, sub is auth-id
         ]
     (when (and ledger (not= (keyword ledger) (keyword iss)))
       (throw (ex-info (str "Invalid ledger in JWT - request originated for ledger: " ledger
                            ", but JWT was issued from ledger: " iss ".")
                       {:status 401
                        :error  :db/invalid-auth})))
     {:ledger      iss
      :auth        sub
      :type        :jwt
      :private-enc pri
      :jwt         jwt})))


(defn fluree-private-from-jwt
  "Extracts a private key from a signed JWT.

  Returns a core async channel that will eventually contain a valid result or exception."
  ([conn jwt] (fluree-private-from-jwt conn jwt nil))
  ([conn jwt ledger]
   (go-try
     (let [auth-map (fluree-auth-map conn ledger jwt)
           {:keys [secret]} (-> conn :meta :password-auth)
           {:keys [ledger auth private-enc]} auth-map
           salt     (<? (salt-from-auth-id conn ledger auth))
           _        (when-not (string? salt)
                      (throw (ex-info (str "Unable to retrieve salt for auth id: " auth ".")
                                      {:status 401
                                       :error  :db/invalid-auth})))
           iv       (->> salt
                         (alphabase/hex->bytes)
                         (take 16))
           private  (crypto/aes-decrypt private-enc iv secret :hex :hex)]
       private))))



(defn fluree-login-user
  "Extracts a private key from a signed JWT.

  Returns a core async channel that will eventually contain a valid result or exception."
  [conn ledger password user-ident auth-ident options]
  (go-try
    (let [auth-ids+salts (if user-ident                     ;; get vector of two-tuples of [auth-id salt]
                           (<? (get-user-auth-ids conn ledger user-ident))
                           ;; just a single auth-ident
                           [[auth-ident (<? (salt-from-auth-id conn ledger auth-ident))]])
          _              (when (empty? auth-ids+salts)
                           (throw (ex-info "No valid auth id/salt available."
                                           {:status 401
                                            :error  :db/invalid-auth})))
          jwt-options    (-> conn :meta :password-auth)]
      (fluree-create-jwt jwt-options ledger password auth-ids+salts options))))


(defn fluree-new-pw-auth
  "Generates and transacts a pw auth record.

  The options is a map which can contain additional data that is part of the transaction:
  - user - Associate auth record to an existing _user.
           Value can be (a) string, assumed to be _user/username,
                        (b) an identity for an existing _user (i.e. subject id or ['_user/username' 'bplatz'])
  - roles - Associate one or more _role directly to this auth record. Value is a vector of identities for _roles
  - expire - Expiration time in epoch ms
  - create-user? - If the provided user is to be created as well with this request
  - private-key - supplied private key to sign request with, else the password signing key will attempt to be used.

  Returns map with following keys:
  - id      - Auth Id generated
  - public  - public key
  - private - private key
  - salt    - salt used
  - jwt     - jwt token with expiration at `expire`, or default expiration (5 min)
  - result  - result of the transaction that added new auth record to db"
  [conn ledger password options]
  (go-try
    (let [jwt-options (-> conn :meta :password-auth)
          {:keys [secret signing-key]} jwt-options
          ;; if an explicit private key is not provided in the options, try to use
          ;; the signing key from the server settings, else tx will attempt to use
          ;; a default root key if one is available
          {:keys [user roles private-key create-user?] :or {private-key signing-key}} options
          kp          (key-pair-from-password secret password nil)
          {:keys [id salt]} kp
          auth-tx     [(util/without-nils
                         {:_id         "_auth$new"
                          :_auth/id    id
                          :_auth/salt  salt
                          :_auth/type  "password-secp256k1"
                          :_auth/roles roles})]
          tx          (cond
                        ;; create a new user
                        (and create-user? (string? user))
                        (conj auth-tx {:_id            "_user$new"
                                       :_user/username user
                                       :_user/auth     ["_auth$new"]})

                        ;; attach to an existing user
                        user
                        (conj auth-tx {:_id        (if (string? user)
                                                     ["_user/username" user]
                                                     user)
                                       :_user/auth ["_auth$new"]})

                        :else
                        auth-tx)
          result      (<? (fdb/transact-async conn ledger tx {:private-key private-key}))]
      (assoc kp :jwt (fluree-create-jwt jwt-options ledger password [[id salt]] options)
                :result result))))






(comment

  (def secret (crypto/random-bytes 32))
  (def kp-with-secret (key-pair-from-password secret "andrew" nil))
  (verify-identity secret "fluree" (:salt kp-with-secret) (:id kp-with-secret))
  (verify-identity secret "bad-password" (:salt kp-with-secret) (:id kp-with-secret))

  kp-with-secret

  (verify-identity secret "andrew" (:salt kp-with-secret) (:id kp-with-secret))



  (key-pair-from-password secret "lois" nil)



  (let [header     {:alg "HS256",
                    :typ "JWT"}
        header-enc (-> header
                       fluree.db.util.json/stringify-UTF8
                       alphabase/bytes->base64)]

    header-enc

    (= header-enc
       "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")

    (-> secret
        alphabase/bytes->base64
        token-auth/base64->base64url)

    )

  (def enc-pri "ce4910db71971911f20c6a5147bae2f27c45b20406f53e3abc9abbcf1f98a81591684344054e35fbd27fc14eab186a70a178ef0f53d28d1bde82154beafbbf91c87b0457785ee0cc69a7e1f98ddab796")
  (def iv (byte-array [12 -35 -96 114 23 73 -24 125 -85 109 -16 -56 16 79 40 66]))
  (def secret (byte-array [-111 -123 2 -98 -15 -97 -7 -16 54 -122 -47 -79 100 -88 60 1 -91 -100 37 -51 -119 7 -49 39 -68 122 29 16 -91 53 -24 -4]))

  (def new-enc (crypto/aes-encrypt "6b7605abb089f63bf8c900026112eacf6ee768f25b80222dbb7731b8573a551e"
                                   iv secret))

  (crypto/aes-decrypt new-enc iv secret :string :hex)
  (vec (alphabase/hex->bytes "6b7605abb089f63bf8c900026112eacf6ee768f25b80222dbb7731b8573a551e"))

  (-> (fluree.crypto/aes-encrypt "6b7605abb089f63bf8c900026112eacf6ee768f25b80222dbb7731b8573a551e" iv secret)
      (fluree.crypto/aes-decrypt iv secret))

  )