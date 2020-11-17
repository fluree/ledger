(ns fluree.db.ledger.consensus.tcp
  (:require [net.async.tcp :as ntcp]
            [clojure.core.async :as async]
            [taoensso.nippy :as nippy]
            [fluree.db.util.async :refer [go-try]]
            [clojure.tools.logging :as log])
  (:import (java.net BindException)))


(defn start-tcp-server
  "Starts a tcp server.
  Returns a zero-argument close-function that can be used to shut down the server."
  [port new-connection-handler]
  (assert (int? port))
  (assert (fn? new-connection-handler))
  (let [evt-loop    (ntcp/event-loop)
        acceptor    (try (ntcp/accept evt-loop {:port          port
                                                :write-chan-fn (fn [] (async/chan (async/dropping-buffer 10)))})
                         (catch BindException _
                           (log/error (str "Cannot start. Port binding failed, address already in use. Port: " port "."))
                           (log/error "FlureeDB Exiting. Adjust your config, or shut down other service using port.")
                           (System/exit 1)))
        accept-chan (:accept-chan acceptor)]
    (async/go-loop []
      (when-let [new-client (async/<! accept-chan)]
        (new-connection-handler new-client)
        (recur)))
    ;; return shutdown function
    (fn []
      (async/close! accept-chan)
      (ntcp/shutdown! evt-loop))))


;;; implementation

(def connections (atom {}))

(defn close-all-connections
  [this-server]
  (let [conns (get-in @connections [this-server :conn-to])]
    (doseq [[remote-server conn] conns]
      (when-let [write-chan (:write-chan conn)]
        (async/close! write-chan))
      (swap! connections update-in [this-server :conn-to] dissoc remote-server))))


(defn- serialize-message
  "Formats our message into byte array with prefixed integer size."
  [header data]
  (nippy/freeze [header data]))


(defn- deserialize-message
  [msg]
  (nippy/thaw msg))


(defn send-message
  [write-chan header data]
  (async/put! write-chan (serialize-message header data)))


(defn send-rpc
  "sends message from this server to remote server.

  Returns true if success, false if not"
  [this-server remote-server header data]
  (let [write-chan (get-in @connections [this-server :conn-to remote-server :write-chan])]
    (if (nil? write-chan)
      false
      (send-message write-chan header data))))


(defn initialize-with-client
  "Checks initial client message and initializes the connection.
  If successful, returns the server-id of the connected client, else
  returns an exception."
  [this-server client]
  (go-try
    (let [{:keys [read-chan write-chan]} client
          timeout-ms         7500
          initialize-timeout (async/timeout timeout-ms)]
      (loop []
        (let [[initial-msg port] (async/alts! [read-chan initialize-timeout])
              [_ data] (if (= port initialize-timeout)
                         nil
                         (try (deserialize-message initial-msg) ;; catch parsing errors (or nil for timeout)
                              (catch Exception _ nil)))
              {:keys [server-id id]} data]
          (cond
            ;; proper initialization
            (and (string? server-id) (int? id))
            (let [conn {:id         id
                        :type       :inbound
                        :from       this-server
                        :to         server-id
                        :write-chan write-chan
                        :read-chan  read-chan}]
              (swap! connections update-in [this-server :conn-to server-id]
                     (fn [existing]
                       (when existing
                         (async/close! (:write-chan existing)))
                       conn))
              conn)

            ;; timed out
            (= port initialize-timeout)
            (do
              (log/warn "Timeout from new client connection that did not send an initialize message.")
              (async/close! write-chan)                     ;; forces connection to close
              nil)

            ;; parsing error or incorrect initialization
            :else
            (do
              (log/info "Waiting to initialize connection, but did not contain proper initialize message"
                        (or data (pr-str initial-msg)))
              (recur))))))))


(defn initialize-with-server
  "Does initialization with a remote server to set up a trusted
  connection.

  Returns connection upon success.

  The initialization process can modify the connection (i.e. add encryption key)"
  [this-server client remote-server]
  (async/go
    (let [{:keys [write-chan read-chan]} client
          conn-id (rand-int (Integer/MAX_VALUE))
          conn    {:id         conn-id
                   :from       this-server
                   :to         remote-server
                   :type       :outbound
                   :read-chan  read-chan
                   :write-chan write-chan}]
      (async/put! write-chan (serialize-message :hello {:server-id this-server :id conn-id}))
      (swap! connections update-in [this-server :conn-to remote-server]
             (fn [existing-conn]
               (when existing-conn                          ;; another process already established connection, close it
                 (async/close! (:write-chan existing-conn)))
               conn))
      conn)))


(defn monitor-remote-connection
  "Monitors remote server connection for messages.
  Used for both server (incoming) clients and outbound client connections.
  In the case of a server, conn will be nil initially."
  [this-server client handler remote-server]
  (let [server? (nil? remote-server)
        {:keys [read-chan]} client]
    (async/go
      (loop [conn nil]
        (let [msg (async/<! read-chan)]
          (cond
            (nil? msg)
            (log/trace "TCP loop closed.")

            ;; status messages are :connected, :disconnected and :closed
            (keyword? msg)
            (let [conn* (case msg
                          :connected
                          (do
                            (log/debug (str "New TCP connection!!" remote-server))
                            (if server?
                              (async/<! (initialize-with-client this-server client))
                              (async/<! (initialize-with-server this-server client remote-server))))

                          :disconnected
                          (let [remote-server (or (:to conn) remote-server)]
                            (log/debug (str "TCP read channel disconnected for server: " (or (:to conn) remote-server)
                                            ". Waiting for reconnect."))
                            (when remote-server
                              (swap! connections assoc-in [this-server :conn-to remote-server] nil))
                            nil)

                          :closed
                          (let [remote-server (or (:to conn) remote-server)]
                            (log/info (str "TCP read channel closed for server: " (or (:to conn) remote-server)
                                           ". Closing TCP loop."))
                            (when remote-server
                              (swap! connections assoc-in [this-server :conn-to remote-server] nil))
                            nil))]
              (if (instance? Exception conn*)
                (log/error conn* (str "Unexpected exception creating connection to: " remote-server))
                (recur conn*)))


            :else
            (do
              (try
                (handler conn msg)
                (catch Exception e
                  (log/error e (str "Error executing handler function for incoming message from client: "
                                    (or (:to conn) remote-server) ". Message: " (pr-str msg)))))
              (recur conn))))))))

;; store client event loop here to be shared across all client connections
(def client-event-loop (atom nil))

(defn get-client-event-loop
  "Will create a new client event loop if one doesn't already exist."
  []
  (or @client-event-loop
      (let [new-state (swap! client-event-loop
                             (fn [existing]
                               (if existing
                                 existing
                                 (ntcp/event-loop))))]
        new-state)))


(defn shutdown-client-event-loop
  "Shuts down client event loop if it exists, returns true."
  []
  (swap! client-event-loop
         (fn [cel]
           (when cel
             (ntcp/shutdown! cel))
           nil))
  true)


(defn launch-client-connection
  "Launches a connection from a client to a server."
  [this-server-cfg remote-server-cfg handler]
  (async/go
    (let [event-loop  (get-client-event-loop)
          this-server (:server-id this-server-cfg)
          {:keys [host port server-id]} remote-server-cfg
          _           (assert (string? host))
          _           (assert (int? port))
          client      (ntcp/connect event-loop {:host       host
                                                :port       port
                                                :write-chan (async/chan (async/dropping-buffer 10))})]
      (monitor-remote-connection this-server client handler server-id))))