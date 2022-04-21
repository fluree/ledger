(ns fluree.db.peer.websocket
  (:require [org.httpkit.server :as http]
            [clojure.core.async :as async]
            [fluree.db.util.log :as log]
            [fluree.db.util.json :as json]
            [fluree.db.permissions-validate :as permissions-validate]
            [fluree.db.peer.messages :as messages]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.util :as util])
  (:import (java.util UUID)))

(set! *warn-on-reflection* true)


(def websocket-timeout
  "Timeout in milliseconds to close websocket if no message received"
  900000)


;; map of current websocket connections
(def ^:private ws-connections (atom {}))


(defn- close-all-sessions
  "Given a websocket id, closes all sessions.

  Returns true if any sessions were closed, else false."
  [ws-id]
  (swap! ws-connections update-in [ws-id :session]
         (fn [sessions-map]
           (doseq [[_ chan] sessions-map]
             (when chan (async/close! chan)))
           ;; nil out sessions
           nil)))


(defn- close-websocket
  "Cleanup when a websocket is closed."
  [ws-id producer-chan]
  (try
    (close-all-sessions ws-id)
    (async/close! producer-chan)
    (swap! ws-connections (fn [ws-state]
                            ;(when-let [send-chan (get-in ws-state [ws-id :send-chan])]
                            ;  (async/close! send-chan))
                            (when-let [ch (get-in ws-state [ws-id :ch])]
                              (http/close ch))
                            (dissoc ws-state ws-id)))
    true
    (catch Exception e
      (log/error e (str "Error closing websocket connection: " ws-id))
      false)))


(defn filter-flakes
  [conn ws-id msg]
  (util/go-try (let [[type [network ledger-id] data] msg
                     auth   (get-in @messages/subscription-auth [ws-id network ledger-id])
                     db     (->> (cond
                                   (= 0 auth) {}
                                   (string? auth) {:auth ["_auth/id" auth]}
                                   :else
                                   {:auth auth})
                                 (fdb/db conn (str network "/" ledger-id))
                                 util/<?)
                     ;; short-circuit flake check when :root? permissions
                     flakes (if (true? (-> db :permissions :root?))
                              (:flakes data)
                              (->> (:flakes data)
                                   (permissions-validate/allow-flakes? db)
                                   (util/<?)))]
                 [type [network ledger-id] {:block (:block data) :t (:t data) :flakes flakes :txns (:txns data)}])))


(defn msg-producer
  "Sends all messages to web socket."
  [system ws-id ch producer-chan]
  (async/go-loop []
    (let [new-msg (async/<! producer-chan)]
      (if (nil? new-msg)
        (do
          ;; producer closed, close web socket if not already
          (log/debug (format "Closing websocket id %s because producer-chan is closed"
                             ws-id))
          (http/close ch))
        (do
          (log/trace "msg-producer received new msg:" new-msg)
          (let [enc-msg (try (if (= :block (first new-msg))
                               (-> (util/<? (filter-flakes (:conn system) ws-id new-msg))
                                   json/stringify)
                               (json/stringify new-msg))
                             (catch Exception _ (log/warn "Unable to json encode outgoing message, dropping: "
                                                          new-msg)))]
            (when enc-msg
              (log/trace "msg-producer sending encoded message:" enc-msg)
              (http/send! ch enc-msg)))
          (recur))))))


(defn start-msg-consumer-loop!
  "Takes a websocket-id, a core.async channel to take incoming websocket
  messages from, and a msg-handler fn to pass parsed incoming messages to.
  Starts a loop to take messages from the consumer chan and pass them to the
  msg-handler. Handles timeouts if no message received. If the consumer-chan
  gets closed, the caller should close the websocket."
  [ws-id consumer-chan msg-handler]
  (async/go-loop []
    (let [[new-msg ch] (async/alts! [consumer-chan (async/timeout websocket-timeout)])]
      (if (= ch consumer-chan)
        (if (nil? new-msg)
          (log/error "Web socket consumer channel closed for websocket id:" ws-id)
          (do
            (-> new-msg json/parse msg-handler)
            (recur)))
        (do
          (async/close! consumer-chan))))))


(defn handler
  [system req]
  (let [ws-id (str (UUID/randomUUID))
        producer-chan (async/chan (async/sliding-buffer 20))
        consumer-chan (async/chan (async/sliding-buffer 20))
        msg-handler #(future
                       (messages/message-handler system producer-chan ws-id %))]
    (start-msg-consumer-loop! ws-id consumer-chan msg-handler)
    (http/with-channel req ch
      (if (http/websocket? ch)
        (do
          (log/info "New socket initiated with id:" ws-id)
          (swap! ws-connections assoc ws-id {:ch            ch
                                             :producer-chan producer-chan})
          (http/on-close ch (fn [status]
                              (log/trace (format "Websocket id %s closed w/ status: %s"
                                                 ws-id status))
                              (close-websocket ws-id producer-chan)))
          (http/send! ch (json/stringify [:set-ws-id nil ws-id]))
          (http/on-receive ch (fn [data]
                                (log/trace (format "Received data on websocket id %s: %s"
                                                   ws-id data))
                                (when-not (async/put! consumer-chan data)
                                  (log/info "Web socket timeout - no pings received. Closing websocket id:" ws-id)
                                  (close-websocket ws-id producer-chan))))

          (msg-producer system ws-id ch producer-chan)
          {:status 200 :body ws-id})
        ;; if it wasn't a valid websocket handshake, return an error
        {:status  400
         :headers {"content-type" "application/text"}
         :body    "Expected a websocket request"}))))

