(ns rebuild.register-ledger-in-raft
  "Register the rebuilt ledger in RAFT state"
  (:require [clojure.java.io :as io]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

(defn find-index-points
  "Find all index root files"
  [base-path network dbid]
  (let [root-dir (io/file base-path "ledger" network dbid "root")]
    (when (.exists root-dir)
      (->> (.listFiles root-dir)
           (map #(.getName %))
           (keep (fn [name]
                  (when-let [m (re-matches #"(\d{15})\.fdbd" name)]
                    (Long/parseLong (second m)))))
           sort))))

(defn find-latest-block
  "Find the latest block number"
  [base-path network dbid]
  (let [block-dir (io/file base-path "ledger" network dbid "block")]
    (when (.exists block-dir)
      (->> (.listFiles block-dir)
           (map #(.getName %))
           (keep (fn [name]
                  (when-let [m (re-matches #"(\d{15})\.fdbd" name)]
                    (Long/parseLong (second m)))))
           sort
           last))))

(defn register-ledger!
  "Register ledger in RAFT with its block and index information"
  [{:keys [base-path network dbid]}]
  (println "\n=== Registering Ledger in RAFT ===")
  (println "Base path:" base-path)
  (println "Network:" network ", DB:" dbid)
  
  ;; Find blocks and indexes
  (let [indexes (find-index-points base-path network dbid)
        latest-block (find-latest-block base-path network dbid)
        latest-index (last indexes)]
    
    (println "Found" (count indexes) "index points:" indexes)
    (println "Latest block:" latest-block)
    (println "Latest index:" latest-index)
    
    ;; Start server to access RAFT
    (let [config (settings/build-env
                  {:fdb-mode                    "ledger"
                   :fdb-storage-type            "file"
                   :fdb-storage-file-root       base-path
                   :fdb-consensus-type          "raft"
                   :fdb-group-servers           "myserver@localhost:11003"
                   :fdb-group-this-server       "myserver"
                   :fdb-group-port              11003
                   :fdb-group-log-directory     (str base-path "/group")
                   :fdb-api-port                9997})
          system (server/startup config)
          conn (:conn system)
          group (:group conn)]
      
      (try
        (println "\nRegistering ledger in RAFT...")
        
        ;; Build indexes map with timestamps
        (let [indexes-map (into {} 
                               (map (fn [idx] [idx (System/currentTimeMillis)])
                                    indexes))]
          
          ;; Register the ledger with all its metadata
          (txproto/kv-assoc-in group 
                               [:networks network :dbs dbid]
                               {:network network
                                :dbid dbid
                                :block latest-block
                                :index latest-index
                                :indexes indexes-map
                                :status :ready
                                :auth {:id "TfCg3qoLH7puKZL8LUfLrTVWiwAe3WPajUc"
                                       :authority ["_auth/id" "TfCg3qoLH7puKZL8LUfLrTVWiwAe3WPajUc"]}})
          
          (println "✓ Ledger registered in RAFT")
          
          ;; Verify registration
          (let [ledger-state (txproto/kv-get-in group [:networks network :dbs dbid])]
            (println "\nVerifying registration:")
            (println "  Block:" (:block ledger-state))
            (println "  Index:" (:index ledger-state))
            (println "  Status:" (:status ledger-state))
            (println "  Index count:" (count (:indexes ledger-state)))))
        
        (finally
          (println "\nShutting down...")
          (server/shutdown system))))))

;; Example usage
(comment
  (register-ledger! {:base-path "/path/to/data"
                     :network "mynetwork"
                     :dbid "mydb"}))