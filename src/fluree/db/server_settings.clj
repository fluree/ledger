(ns fluree.db.server-settings
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.storage.filestore :as filestore]
            [fluree.db.ledger.storage.memorystore :as memorystore]
            [fluree.db.ledger.storage.s3store :as s3store]
            [fluree.db.serde.avro :as avro]
            [fluree.db.serde.none :as none]
            [clojure.core.async :as async]
            [fluree.db.ledger.stats :as stats]
            [fluree.crypto :as crypto]
            [fluree.crypto.util :refer [hash-string-key]])
  (:import (java.util Hashtable$Entry Properties)
           (java.lang.management ManagementFactory)
           (java.io Reader)))

(set! *warn-on-reflection* true)


;; note every environment variable must be placed in this default map for it to be picked up.
;; THIS IS THE MASTER LIST!  nil as a value means there is no default
(def default-env
  {:fdb-mode                     "query"                    ;; dev, query, ledger
   :fdb-join?                    false                      ;; set to true when server is joining an existing/running network
   :fdb-license-key              nil
   :fdb-consensus-type           "raft"                     ;; raft
   :fdb-encryption-secret        nil                        ;; Text encryption secret for encrypting data at rest and in transit
   :fdb-json-bigdec-string       true                       ;; encode BigDecimal numbers as a string for json.stringify

   :fdb-group-config-path        "./"
   :fdb-group-private-key        nil
   :fdb-group-private-key-file   nil                        ;; optional location of file that contains group private key

   ;; ledger group settings
   :fdb-group-servers            nil                        ;; list of server-id@host:port, separated by commas, of servers to connect to.
   :fdb-group-this-server        nil                        ;; id of this server, must appear in the fdb-group-servers above
   :fdb-group-timeout            2000                       ;; start new election if nothing from leader in this number of milliseconds
   :fdb-group-heartbeat          nil                        ;; defaults to 1/3 of tx-group-timeout-ms
   :fdb-group-catch-up-rounds    10                         ;; defaults to 1/3 of tx-group-timeout-ms
   :fdb-group-log-directory      "./data/group"             ;; where to store raft logs for the group
   :fdb-group-snapshot-path      "data/group/snapshots"     ;; where in configured storage to store raft snapshots
   :fdb-group-snapshot-threshold 200                        ;; how many new index entries before creating a new snapshot
   :fdb-group-log-history        5                          ;; number of historical log files to keep around

   :fdb-storage-type             "file"                     ;; file, memory, s3
   :fdb-storage-file-root        "./data/"
   :fdb-storage-file-directory   nil                        ;; deprecated in favor of fdb-storage-file-root
   :fdb-storage-file-group-path  "group/"
   :fdb-storage-file-ledger-path "ledger/"

   :fdb-storage-s3-bucket        nil
   :fdb-storage-s3-group-prefix  "group/"
   :fdb-storage-s3-ledger-prefix "ledger/"

   :fdb-memory-cache             "200mb"
   :fdb-memory-reindex           "1mb"
   :fdb-memory-reindex-max       "2mb"
   :fdb-stats-report-frequency   "1m"

   ;; api settings
   :fdb-api-port                 8090                       ;; integer
   :fdb-api-open                 true                       ;; true or false

   :fdb-ledger-port              9790                       ;; port this server will listen on for group messages
   :fdb-ledger-private-keys      nil
   :fdb-ledger-servers           nil

   ;; password authentication settings
   :fdb-pw-auth-enable           true
   :fdb-pw-auth-secret           "fluree"
   :fdb-pw-auth-jwt-secret       nil
   :fdb-pw-auth-signing-key      nil
   :fdb-pw-auth-jwt-max-exp      "1y"
   :fdb-pw-auth-jwt-max-renewal  "1y"})


;; FDB_SETTINGS: dev
(def env-dev-defaults
  {:fdb-mode                   "dev"
   :fdb-storage-type           "file"

   :fdb-logs-type              "memory"
   :fdb-memory-cache           ".5gb"
   :fdb-memory-reindex         "100kb"
   :fdb-memory-reindex-max     "1mb"
   :fdb-stats-report-frequency "10m"
   ;:fdb-debug-mode             false
   ;; group settings
   :fdb-group-servers          "ABC@localhost:12300,DEF@localhost:12301"
   :fdb-group-this-server      "DEF"

   ;; api settings
   :fdb-api-open               true})


(defn- read-properties-file
  "Reads properties file at file-name, if it doesn't exist returns nil."
  [file-name]
  (let [file (io/file file-name)]
    (when (.exists file)
      (with-open [^Reader reader (io/reader file)]
        (let [props (Properties.)]
          (.load props reader)
          (->> (for [prop props]
                 (let [k (.getKey ^Hashtable$Entry prop)
                       v (.getValue ^Hashtable$Entry prop)]
                   (if (= "" v)
                     nil
                     [(keyword k) (if (= "" v) nil v)])))
               (into {})
               (util/without-nils)))))))


(defn build-env
  "Builds final environment map.
  Uses either/both FDB_MODE and FDB_SETTINGS variables to apply
  a set of default values if not otherwise specified."
  [environment]
  (let [properties      (when-let [prop-file (:fdb-properties-file environment)]
                          (read-properties-file prop-file))
        java-prop-flags (-> (stats/jvm-arguments) :input stats/jvm-args->map)
        _               (if properties
                          (log/info (format "Properties file %s successfully loaded."
                                            (:fdb-properties-file environment)))
                          (log/info "Properties file does not exist, skipping."))
        propEnvFlag     (merge properties java-prop-flags environment)
        propEnvFlagDef  (reduce
                          (fn [acc [k v]] (assoc acc k (or (get propEnvFlag k) v)))
                          {}
                          default-env)]
    (assert (#{"query" "ledger" "dev"} (-> propEnvFlagDef :fdb-mode str/lower-case))
            "Invalid FDB_MODE, must be dev, query or ledger.")
    propEnvFlagDef))


(defn env-boolean
  [x]
  (cond
    (boolean? x) x
    (string? x) (case (str/lower-case x)
                  "true" true
                  "false" false
                  "t" true
                  "f" false)
    :else false))


(defn env-integer
  [x]
  (when x
    (if (string? x)
      (Integer/parseInt x)
      (int x))))


(defn env-float
  [x]
  (when x
    (if (string? x)
      (Float/parseFloat x)
      (float x))))


(defn parse-time-string
  "Returns a two-tuple of [double unit-string]"
  [s]
  (let [[_ n unit] (re-find #"^([0-9.]+)([sSmMhHdDyY]{0,2})$" s)
        unit* (if (empty? unit)
                "ms"                                        ;; default to unit of ms
                (str/lower-case unit))]
    (when (or (nil? n)
              (and unit* (not (#{"ms" "s" "m" "h" "d" "y"} unit*))))
      (throw (Exception. ^String
                         (str "Invalid time unit provided in environment. "
                              "Should be a number optionally followed by units of ms (default), s, m, h, d or y. "
                              "Provided: " s))))
    [(Double/parseDouble n) unit*]))


(defn env-milliseconds
  "Converts an env value into milliseconds"
  [x]
  (when x
    (let [[n unit] (cond
                     (pos-int? x) [x "ms"]
                     (string? x) (parse-time-string x)
                     :else
                     (throw (Exception. ^String
                                        (str "Invalid time unit provided in environment. "
                                             "Should be a number optionally followed by units of ms (default), s, m, h, d or y. "
                                             "Provided: " x))))]
      (-> (case unit
            "ms" n
            "s" (* n 1000)
            "m" (* n 60000)
            "h" (* n 3600000)
            "d" (* n 86400000)
            "y" (* n 31536000000))
          (double)
          (Math/round)))))


(defn env-seconds
  "Converts an env value into seconds"
  [x]
  (-> (env-milliseconds x)
      (/ 1000)
      (double)
      (Math/round)))


(defn parse-size-string
  "Returns a two-tuple of [double unit-string]"
  [s]
  (let [[_ n unit] (re-find #"^([0-9.]+)([bBkKmMgG]{0,2})$" s)
        unit* (if (empty? unit)
                "b"                                         ;; default to bytes (b)
                (str/lower-case unit))]
    (when (or (nil? n)
              (and unit* (not (#{"b" "k" "kb" "m" "mb" "g" "gb"} unit*))))
      (throw (Exception. ^String
                         (str "Invalid size unit provided in environment. "
                              "Should be a number optionally followed by units of b (default), k/kb, m/mb, or g/gb. "
                              "Provided: " s))))
    [(Double/parseDouble n) unit*]))


(defn env-bytes
  "Converts provided input to quantity in bytes. Input can be string with a numbers followed
  by a unit which can be any of:
  - g or gb - gigabytes i.e. 0.5gb or 1g
  - m or mb - megabytes i.e. 1mb or 1.4m
  - k or kb - kilobytes i.e. 100k or 163kb
  - b       - bytes     i.e. 512b

  If a positive integer is provided, it is assumed it is for bytes and is not converted."
  [x]
  (let [[n unit] (cond
                   (pos-int? x)
                   [x "b"]

                   (string? x)
                   (parse-size-string x)

                   :else
                   (throw (Exception. ^String
                                      (str "Invalid time unit provided in environment. "
                                           "Should be a number optionally followed by units "
                                           "of b (default), k, kb, m, mb, g, or gb. Provided: " x))))]
    (-> (case unit
          "b" n
          "k" (* n 1000)
          "kb" (* n 1000)
          "m" (* n 1000000)
          "mb" (* n 1000000)
          "g" (* n 1000000000)
          "gb" (* n 1000000000))
        (double)
        (Math/round))))



(defn- env-storage-type
  [env]
  (-> env :fdb-storage-type str/lower-case keyword))


(defn- get-or-generate-tx-private-key
  [env]
  (or (when-let [priv (:fdb-group-private-key env)]
        (log/debug "Using private key from fdb-group-private-key config string")
        (str/trim priv))
      (let [priv-key-file (or (:fdb-group-private-key-file env)
                              "default-private-key.txt")
            file          (io/file (:fdb-group-config-path env) priv-key-file)
            exists?       (.exists file)]
        (if exists?
          (do
            (log/debug "Using private key from file" (.toString file))
            (-> file slurp str/trim))
          ;; make sure private key generated is 64 characters
          (let [{:keys [private]} (some #(when (= 64 (count (:private %))) %) (repeatedly crypto/generate-key-pair))]
            (log/debug "Generating new private key and storing in" (.toString file))
            (spit file private)
            private)))))


(defn- get-cache-size
  [env]
  (let [mem-cache-bytes (-> env :fdb-memory-cache env-bytes)
        cache-size      (quot mem-cache-bytes 1000000)
        memory-mxbean   (-> (ManagementFactory/getMemoryMXBean)
                            (.getHeapMemoryUsage))
        max-memory      (.getMax memory-mxbean)]
    ;; for now use 1000 per 1GB size... or bytes /
    (when (< mem-cache-bytes 10000000)
      (throw (ex-info (str "Unable to start, fdb-memory-cache size configured too low at: " (:fdb-memory-cache env)
                           ". Must be at least 100MB")
                      {:status 400 :error :db/invalid-configuration}))
      (System/exit 1))

    (when (< max-memory (* 1.2 mem-cache-bytes))
      (throw (ex-info (str "Unable to start, JVM max memory must be at least 20% larger than fdb-memory-cache size (ideally more). Cache size: "
                           (:fdb-memory-cache env) ". JVM max memory:" (format "%.1f GB" (/ max-memory 1073741824.0)))
                      {:status 400 :error :db/invalid-configuration}))
      (System/exit 1))
    cache-size))


(defn- get-serializer
  [env]
  (let [typ        (env-storage-type env)
        serde-type (case typ
                     :memory :none
                     ;; else
                     :avro)
        serde-opts {}]
    (case serde-type
      :avro
      (avro/map->Serializer serde-opts)

      :none
      (none/map->Serializer serde-opts))))


(defn password-feature-settings
  "Returns settings for password based auth or nil if not enabled."
  [settings]
  (let [{:keys [fdb-pw-auth-enable fdb-pw-auth-secret
                fdb-pw-auth-jwt-secret fdb-pw-auth-signing-key
                fdb-pw-auth-jwt-max-exp fdb-pw-auth-jwt-max-renewal]} settings]
    (when (env-boolean fdb-pw-auth-enable)
      (let [secret     (crypto/sha3-256-normalize fdb-pw-auth-secret :bytes)
            jwt-secret (when (not-empty fdb-pw-auth-jwt-secret)
                         (crypto/sha3-256-normalize fdb-pw-auth-jwt-secret))]
        {:secret      secret
         :jwt-secret  (or jwt-secret secret)
         :signing-key (not-empty fdb-pw-auth-signing-key)
         :max-exp     (env-milliseconds fdb-pw-auth-jwt-max-exp)
         :max-renewal (env-milliseconds fdb-pw-auth-jwt-max-renewal)}))))


(defn- encryption-secret->key
  "Returns the byte-array version of the encryption secret from the settings if utilized."
  [settings]
  (some-> settings :fdb-encryption-secret (hash-string-key 32) byte-array))


(defn canonicalize-path
  "Ensures that `path` arg starts with either `./` or `/`. If it starts with
  any other character, `./` will be prepended to the returned string. Otherwise
  the original arg will be returned."
  [path]
  (if (boolean (re-find #"^[./]" path))
    path
    (str "./" path)))


(defn- file-storage-path*
  "Once we remove support for :fdb-storage-file-directory, replace
  file-storage-path with this fn, adopt its docstring, and make it public."
  [type env]
  (when (= :file (env-storage-type env))
    (let [type-kw       (keyword (str "fdb-storage-file-" (name type) "-path"))
          append-slash  #(str % "/")
          config-dir    (:fdb-storage-file-root env)
          canonical-dir (canonicalize-path config-dir)]
      (->> (type-kw env)
           (io/file canonical-dir)
           .toString
           append-slash))))


(defn file-storage-path
  "Returns the full canonicalized path for local FS storage of the given type.
  Returns nil if FS storage isn't being used."
  [type env]
  (when (= :file (env-storage-type env))
    (if (= :ledger (keyword type))
      (if-let [old-ledger-dir (:fdb-storage-file-directory env)]
        (do
          (log/warn (str "fdb-storage-file-directory is deprecated. "
                         "Please use fdb-storage-file-root instead. "
                         "Usually you can just set it to the parent directory "
                         "(i.e. if you have fdb-storage-file-directory=data/ledger, "
                         "change it to fdb-storage-file-root=./data)."))
          old-ledger-dir)
        (file-storage-path* type env))
      (file-storage-path* type env))))


(defn generate-conn-settings
  [settings]
  (let [encryption-key           (encryption-secret->key settings)
        dev?                     (= "dev" (some-> settings :fdb-mode str/lower-case))
        is-transactor?           (boolean (#{"dev" "ledger"} (some-> settings :fdb-mode str/lower-case)))
        storage-type             (env-storage-type settings)
        s3-conn                  (some-> settings :fdb-storage-s3-bucket s3store/connect)
        file-ledger-storage-path (file-storage-path :ledger settings)
        _                        (log/debug "generate-conn-settings file-ledger-storage-path:" file-ledger-storage-path)
        s3-ledger-storage-prefix (:fdb-storage-s3-ledger-prefix settings)
        storage-read             (case storage-type
                                   :file (filestore/connection-storage-read
                                           file-ledger-storage-path
                                           encryption-key)
                                   :s3 (s3store/connection-storage-read
                                         s3-conn
                                         s3-ledger-storage-prefix
                                         encryption-key)
                                   :memory memorystore/connection-storage-read)
        storage-exists           (case storage-type
                                   :file (filestore/connection-storage-exists?
                                           file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-exists?
                                         s3-conn s3-ledger-storage-prefix)
                                   :memory memorystore/connection-storage-read)
        storage-write            (case storage-type
                                   :file (filestore/connection-storage-write
                                           file-ledger-storage-path
                                           encryption-key)
                                   :s3 (s3store/connection-storage-write
                                         s3-conn s3-ledger-storage-prefix
                                         encryption-key)
                                   :memory memorystore/connection-storage-write)
        storage-rename           (case storage-type
                                   :file (filestore/connection-storage-rename
                                           file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-rename
                                         s3-conn s3-ledger-storage-prefix)
                                   :memory nil)
        storage-list             (case storage-type
                                   :file (filestore/connection-storage-list
                                           file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-list
                                         s3-conn s3-ledger-storage-prefix)
                                   :memory nil)
        serializer               (get-serializer settings)
        close-fn                 (case storage-type
                                   :file (fn [] nil)
                                   :memory #(memorystore/close)
                                   :s3 #(s3store/close s3-conn))]
    {:storage-type storage-type
     :servers      (:fdb-conn-servers settings)
     :options      {:transactor?    is-transactor?
                    :storage-read   storage-read
                    :storage-exists storage-exists
                    ;; Storage write is overwritten in the default transactor to use the RAFT group as part of the write process
                    ;; A default can be used here for
                    :storage-write  storage-write
                    :storage-rename storage-rename
                    :storage-list   storage-list

                    :req-chan       (async/chan)            ;; create our own request channel so we can monitor it if in 'dev' mode
                    ;:sub-chan            nil
                    ;:object-cache        nil
                    ;:object-cache-size   nil
                    :memory         (some-> settings :fdb-memory-cache env-bytes)
                    :close-fn       close-fn
                    :serializer     serializer

                    ;; ledger-specific settings
                    :tx-private-key (get-or-generate-tx-private-key settings)
                    ;; meta is a map of settings that are implementation-specific, i.e.
                    ;; a transactor needs novelty-min and novelty-max, a web browser connection might need some different info
                    :meta           {:novelty-min       (-> settings :fdb-memory-reindex env-bytes)
                                     :novelty-max       (-> settings :fdb-memory-reindex-max env-bytes)
                                     :dev?              dev?
                                     :password-auth     (password-feature-settings settings)
                                     :open-api          (-> settings :fdb-api-open env-boolean)
                                     :file-storage-path (when (= :file storage-type)
                                                          file-ledger-storage-path)
                                     :s3-storage        (when (= :s3 storage-type)
                                                          {:bucket (:bucket s3-conn)
                                                           :prefix s3-ledger-storage-prefix})
                                     :encryption-secret encryption-key}}}))


(defn- build-group-server-configs
  "Takes settings and returns list of group servers properly formatted."
  [settings]
  (let [this-server (:fdb-group-this-server settings)
        servers     (->> (str/split (:fdb-group-servers settings) #"[,;]")
                         (reduce (fn [acc server]
                                   (let [[_ server-id host port] (re-find #"^([^@]+)@([^:]+):([0-9]+)$" server)]
                                     (when-not (and server-id host port)
                                       (throw (ex-info (str "Invalid group server provided: " server ". Must be in the format of server-id@hostname:port with each server separated by a comma.")
                                                       {:status 400 :error :db/invalid-configuration})))
                                     (conj acc {:server-id server-id :host host
                                                :port      (Integer/parseInt ^String port)})))
                                 []))
        ;; add in "me", and use existing host/port if provided, otherwise use localhost:fdb-group-port
        servers+me  (if (some #(when (= this-server (:server-id %)) %) servers)
                      ;; config has this-server already in it
                      servers
                      ;; need to add this-server to config
                      (conj servers {:server-id this-server
                                     :port      (or (:fdb-group-port settings)
                                                    (throw (ex-info (format
                                                                      (str "Port for this server (%s) not provided. "
                                                                           "Must be found in fdb-group-servers or "
                                                                           "fdb-group-port environment/config variables.")
                                                                      this-server)
                                                                    {:status 400
                                                                     :error  :db/invalid-configuration})))}))]
    servers+me))


(defn- build-group-settings
  "Generates groups settings.
  Raft servers should be a map, with server-ids as keys and values as a map with :host and :port keys."
  [settings group-servers]
  (let [this-server              (:fdb-group-this-server settings)
        timeout-ms               (env-milliseconds (:fdb-group-timeout settings))
        heartbeat-ms             (if-let [fdb-group-heartbeat (:fdb-group-heartbeat settings)]
                                   (env-milliseconds fdb-group-heartbeat)
                                   (quot (env-milliseconds (:fdb-group-timeout settings)) 3))
        _                        (when (> heartbeat-ms timeout-ms)
                                   (throw (ex-info (format "TX group heartbeat milliseconds: %s cannot be greater than timeout milliseconds: %s." timeout-ms heartbeat-ms)
                                                   {:status 400
                                                    :error  :db/invalid-configuration})))
        private-keys             (into [] (->> (get-or-generate-tx-private-key settings)
                                               (str/split-lines)
                                               (filter not-empty)
                                               (reduce #(if (str/includes? %2 ",")
                                                          (into %1 (str/split %2 #","))
                                                          (conj %1 %2)) [])))
        encryption-key           (encryption-secret->key settings)
        storage-type             (env-storage-type settings)
        s3-conn                  (some-> settings :fdb-storage-s3-bucket s3store/connect)
        file-ledger-storage-path (file-storage-path :ledger settings)
        file-group-storage-path  (file-storage-path :group settings)
        s3-ledger-storage-prefix (:fdb-storage-s3-ledger-prefix settings)
        s3-group-storage-prefix  (:fdb-storage-s3-group-prefix settings)
        storage-ledger-write     (case storage-type
                                   (:file :memory) (filestore/connection-storage-write
                                                     file-ledger-storage-path
                                                     encryption-key)
                                   :s3 (s3store/connection-storage-write
                                         s3-conn
                                         s3-ledger-storage-prefix
                                         encryption-key))
        storage-group-write      (case storage-type
                                   (:file :memory) (filestore/connection-storage-write
                                                     file-group-storage-path
                                                     encryption-key)
                                   :s3 (s3store/connection-storage-write
                                         s3-conn
                                         s3-group-storage-prefix
                                         encryption-key))
        storage-ledger-read      (case storage-type
                                   (:file :memory) (filestore/connection-storage-read
                                                     file-ledger-storage-path
                                                     encryption-key)
                                   :s3 (s3store/connection-storage-read
                                         s3-conn
                                         s3-ledger-storage-prefix
                                         encryption-key))
        storage-group-read       (case storage-type
                                   (:file :memory) (filestore/connection-storage-read
                                                     file-group-storage-path
                                                     encryption-key)
                                   :s3 (s3store/connection-storage-read
                                         s3-conn
                                         s3-group-storage-prefix
                                         encryption-key))
        storage-ledger-rename    (case storage-type
                                   (:file :memory) (filestore/connection-storage-rename
                                                     file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-rename
                                         s3-conn s3-ledger-storage-prefix))
        storage-group-rename     (case storage-type
                                   (:file :memory) (filestore/connection-storage-rename
                                                     file-group-storage-path)
                                   :s3 (s3store/connection-storage-rename
                                         s3-conn s3-group-storage-prefix))
        storage-ledger-exists    (case storage-type
                                   (:file :memory) (filestore/connection-storage-exists?
                                                     file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-exists?
                                         s3-conn s3-ledger-storage-prefix))
        storage-group-exists     (case storage-type
                                   (:file :memory) (filestore/connection-storage-exists?
                                                     file-group-storage-path)
                                   :s3 (s3store/connection-storage-exists?
                                         s3-conn s3-group-storage-prefix))
        storage-ledger-delete    (case storage-type
                                   (:file :memory) (filestore/connection-storage-delete
                                                     file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-delete
                                         s3-conn s3-ledger-storage-prefix))
        storage-group-delete     (case storage-type
                                   (:file :memory) (filestore/connection-storage-delete
                                                     file-group-storage-path)
                                   :s3 (s3store/connection-storage-delete
                                         s3-conn s3-group-storage-prefix))
        storage-ledger-list      (case storage-type
                                   (:file :memory) (filestore/connection-storage-list
                                                     file-ledger-storage-path)
                                   :s3 (s3store/connection-storage-list
                                         s3-conn s3-ledger-storage-prefix))
        storage-group-list       (case storage-type
                                   (:file :memory) (filestore/connection-storage-list
                                                     file-group-storage-path)
                                   :s3 (s3store/connection-storage-list
                                         s3-conn s3-group-storage-prefix))]
    {:server-configs        group-servers                   ;; list of server-id@host:port, separated by commas, of servers to connect to. Can include or exclude this server (fdb-group-this-server).
     :this-server           this-server                     ;; id of this server
     :port                  (get-in group-servers [this-server :port]) ;; port this server will listen on for group messages
     :timeout-ms            timeout-ms                      ;; start new election if nothing from leader in this number of milliseconds
     :heartbeat-ms          heartbeat-ms                    ;; defaults to 1/3 of tx-group-timeout-ms
     :catch-up-rounds       (env-integer (:fdb-group-catch-up-rounds settings)) ;; defaults to 10
     :log-history           (env-integer (:fdb-group-log-history settings)) ;; number of historical log files to keep around
     :snapshot-threshold    (env-integer (:fdb-group-snapshot-threshold settings)) ;; how many new index entries before creating a new snapshot
     :private-keys          (not-empty private-keys)        ;; Transactor group private key(s). Separate multiple keys with commas. These keys will be replicated to all servers in the group, which they can use as identities for external networks or as defaults for DBs. They only need to be provided one time.
     :open-api              (-> settings :fdb-api-open env-boolean)
     :log-directory         (-> settings :fdb-group-log-directory canonicalize-path) ;; where to store raft logs for the group
     :snapshot-path         (:fdb-group-snapshot-path settings) ;; where in storage to store raft snapshots
     :storage-type          storage-type
     :storage-ledger-write  storage-ledger-write
     :storage-ledger-read   storage-ledger-read
     :storage-ledger-rename storage-ledger-rename
     :storage-ledger-exists storage-ledger-exists
     :storage-ledger-delete storage-ledger-delete
     :storage-ledger-list   storage-ledger-list

     :storage-group-write   storage-group-write
     :storage-group-read    storage-group-read
     :storage-group-rename  storage-group-rename
     :storage-group-exists  storage-group-exists
     :storage-group-delete  storage-group-delete
     :storage-group-list    storage-group-list}))


(defn raft-transactor-settings
  [settings]
  {:transactors (->> (str/split (:fdb-group-servers settings) #",")
                     (remove #(= % (:fdb-group-this-server settings))))
   :me          (:fdb-group-this-server settings)})


(defn build-settings
  [settings]
  (let [fdb-mode       (-> settings :fdb-mode str/lower-case)
        fdb-join       (-> settings :fdb-join? env-boolean)
        is-ledger?     (boolean (#{"dev" "ledger"} fdb-mode))
        webserver?     (boolean (#{"dev" "query" "ledger"} fdb-mode))
        debug-mode?    (-> settings :fdb-debug-mode env-boolean)
        ;fdb-version         (util/get-version "fluree" "db")
        consensus-type (-> settings :fdb-consensus-type str/lower-case keyword)
        hostname       (-> settings :hostname)
        group-servers  (build-group-server-configs settings)]

    {:transactor? is-ledger?
     :join?       fdb-join
     :dev?        (= "dev" fdb-mode)
     :mode        fdb-mode
     :hostname    hostname
     ;:version     fdb-version
     :stats       {:interval (-> settings :fdb-stats-report-frequency env-milliseconds)}
     :conn        (generate-conn-settings settings)

     :cache       {:idx-cache-size       (get-cache-size settings)
                   ;; block-size stores pre-computed index segments at a specific block
                   :idx-block-cache-size (quot (get-cache-size settings) 10)}
     :webserver   {:port        (env-integer (:fdb-api-port settings))
                   ;; we only run web server on the query engines
                   :enabled     webserver?
                   :debug-mode? debug-mode?
                   :open-api    (-> settings :fdb-api-open env-boolean)
                   :json-bigdec-string
                                (-> settings :fdb-json-bigdec-string env-boolean)
                   :meta        {:hostname hostname}}
     ;:version  fdb-version

     :group       (build-group-settings settings group-servers)
     :consensus   {:type    consensus-type
                   :options (case consensus-type
                              :raft (raft-transactor-settings settings)
                              :in-memory {})}}))


(comment

  (require '[environ.core :as environ])
  environ.core/env

  (->
    (build-env environ/env)))
