(ns fluree.db.peer.websocket
  (:require [manifold.deferred :as d]
            [aleph.http :as http]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [fluree.db.util.json :as json]
            [fluree.db.permissions-validate :as permissions-validate]
            [fluree.db.peer.messages :as messages]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.util :as util])
  (:import (java.util UUID)))

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
                            (when-let [socket (get-in ws-state [ws-id :ws])]
                              (s/close! socket))
                            (dissoc ws-state ws-id)))
    true
    (catch Exception e
      (log/error e (str "Error closing websocket connection: " ws-id))
      false)))


(defn filter-flakes
  [conn ws-id msg]
  (util/go-try (let [[type [network dbid] data] msg
                     auth   (get-in @messages/subscription-auth [ws-id network dbid])
                     db     (->> (cond
                                   (= 0 auth) {}
                                   (string? auth) {:auth ["_auth/id" auth]}
                                   :else
                                   {:auth auth})
                                 (fdb/db conn (str network "/" dbid))
                                 util/<?)
                     ;; short-circuit flake check when :root? permissions
                     flakes (if (true? (-> db :permissions :root?))
                              (:flakes data)
                              (->> (:flakes data)
                                   (permissions-validate/allow-flakes? db)
                                   (util/<?)))]
                 [type [network dbid] {:block (:block data) :t (:t data) :flakes flakes :txns (:txns data)}])))


(defn msg-producer
  "Sends all messages to web socket."
  [system ws-id ws producer-chan]
  (async/go-loop []
    (let [new-msg (async/<! producer-chan)]
      (if (nil? new-msg)
        ;; producer closed, close web socket if not already
        (s/close! ws)
        (do
          (let [enc-msg (try (if (= :block (first new-msg))
                               (-> (util/<? (filter-flakes (:conn system) ws-id new-msg))
                                   json/stringify)
                               (json/stringify new-msg))
                             (catch Exception _ (log/warn "Unable to json encode outgoing message, dropping: "
                                                          (pr-str new-msg))))]
            (when enc-msg @(s/put! ws enc-msg)))
          (recur))))))


(defn msg-consumer
  "Consumes all messages from web socket"
  [system ws-id ws producer-chan]
  (d/loop []
          (d/chain (d/timeout! (s/take! ws) 90000 ::timeout)
                   #(cond
                      (nil? %)
                      (log/info "Web socket closed: " ws-id)

                      (= ::timeout %)
                      (do
                        (log/info "Web socket timeout - no pings received. Closing: " ws-id)
                        (s/close! ws))

                      :else
                      (do
                        (->> (json/parse %)
                             (messages/message-handler system producer-chan ws-id))
                        (d/recur))))))


(defn handler
  [system req]
  (d/let-flow [ws (d/catch
                    (http/websocket-connection req {:max-frame-payload 1e8 :max-frame-size 2e8})
                    (fn [_] nil))]
              (if ws
                (let [ws-id         (str (UUID/randomUUID))
                      producer-chan (async/chan (async/sliding-buffer 20))]
                  (log/info (str "New socket initiated with id: " ws-id))
                  (swap! ws-connections assoc ws-id {:ws            ws
                                                     :producer-chan producer-chan})
                  ;; register callback for closed socket
                  (s/on-closed ws (fn [] (close-websocket ws-id producer-chan)))
                  ;; register message handler for requests, create a buffer to allow some processing time.
                  ;; send back a connection message with assigned ws-id
                  (d/chain (s/put! ws (json/stringify [:set-ws-id nil ws-id]))
                           #(when (false? %)
                              ;; failed, close
                              (s/close! ws)))
                  (msg-producer system ws-id ws producer-chan)
                  (msg-consumer system ws-id ws producer-chan)

                  ;(s/consume (partial message-handler system ws ws-id) (s/buffer 25 ws))
                  {:status 200 :body ws-id})
                ;; if it wasn't a valid websocket handshake, return an error
                {:status  400
                 :headers {"content-type" "application/text"}
                 :body    "Expected a websocket request."})))