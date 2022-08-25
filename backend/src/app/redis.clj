;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.redis
  "The msgbus abstraction implemented using redis as underlying backend."
  (:require
   [app.common.data :as d]
   [app.common.exceptions :as ex]
   [app.common.logging :as l]
   [app.common.spec :as us]
   [app.common.transit :as t]
   [app.config :as cfg]
   [app.util.async :as aa]
   [app.util.time :as dt]
   [app.worker :as wrk]
   [clojure.core.async :as a]
   [clojure.spec.alpha :as s]
   [integrant.core :as ig]
   [promesa.core :as p])
  (:import
   io.lettuce.core.RedisClient
   io.lettuce.core.RedisURI
   io.lettuce.core.api.StatefulConnection
   io.lettuce.core.api.StatefulRedisConnection
   io.lettuce.core.api.async.RedisAsyncCommands
   io.lettuce.core.codec.ByteArrayCodec
   io.lettuce.core.codec.RedisCodec
   io.lettuce.core.codec.StringCodec
   io.lettuce.core.pubsub.RedisPubSubListener
   io.lettuce.core.pubsub.StatefulRedisPubSubConnection
   io.lettuce.core.pubsub.api.sync.RedisPubSubCommands
   io.lettuce.core.resource.ClientResources
   io.lettuce.core.resource.DefaultClientResources
   java.time.Duration))

(set! *warn-on-reflection* true)

(declare initialize-resources)
(declare connect)

(s/def ::connection
  #(instance? StatefulRedisConnection %))

(s/def ::pubsub-connection
  #(instance? StatefulRedisPubSubConnection %))

(s/def ::resources
  #(instance? ClientResources %))

(s/def ::pubsub-listener
  #(instance? RedisPubSubListener %))

(s/def ::redis (s/keys :req [::resources]))

(s/def ::uri ::us/not-empty-string)
(s/def ::timeout ::dt/duration)
(s/def ::connect? ::us/boolean)
(s/def ::io-threads ::us/integer)
(s/def ::worker-threads ::us/integer)

(defmethod ig/pre-init-spec ::redis [_]
  (s/keys :req-un [::uri]
          :opt-un [::timeout
                   ::connect?
                   ::io-threads
                   ::worker-threads]))

;; Runtime.getRuntime().availableProcessors()

(defmethod ig/prep-key ::redis
  [_ cfg]
  (merge {:timeout (dt/duration 5000)
          :io-threads 4
          :worker-threads 4}
         (d/without-nils cfg)))

(defmethod ig/init-key ::redis
  [_ {:keys [connect?] :as cfg}]
  (let [cfg (initialize-resources cfg)]
    (cond-> cfg
      connect? (assoc ::connection (connect cfg)))))

(defmethod ig/halt-key! ::redis
  [_ {:keys [::resources ::connection]}]
  (when connection
    (.close ^java.lang.AutoCloseable connection))
  (when resources
    (.shutdown ^ClientResources resources)))

(def default-codec
  (RedisCodec/of StringCodec/UTF8 ByteArrayCodec/INSTANCE))

(defn- initialize-resources
  "Initialize redis connection resources"
  [{:keys [uri io-threads worker-threads connect?] :as cfg}]
  (l/info :hint "initialize redis resources"
          :uri uri
          :io-threads io-threads
          :worker-threads worker-threads
          :connect? connect?)

  (let [resources (.. (DefaultClientResources/builder)
                      (ioThreadPoolSize ^long io-threads)
                      (computationThreadPoolSize ^long worker-threads)
                      (build))

        redis-uri (RedisURI/create ^String uri)]
    (-> cfg
        (assoc ::redis-uri redis-uri)
        (assoc ::resources resources))))

(defn- shutdown-resources
  [{:keys [::resources]}]
  (when resources
    (.shutdown ^ClientResources resources)))

(defn connect
  [{:keys [::resources ::redis-uri] :as cfg}
   & {:keys [timeout codec pubsub?] :or {codec default-codec}}]

  (us/assert! ::resources resources)

  (let [client  (RedisClient/create ^ClientResources resources ^RedisURI redis-uri)
        timeout (or timeout (:timeout cfg))

        conn    (if pubsub?
                  (.connectPubSub ^RedisClient client ^RedisCodec codec)
                  (.connect ^RedisClient client ^RedisCodec codec))]

    (.setTimeout ^StatefulConnection conn ^Duration timeout)

    (reify
      clojure.lang.IDeref
      (deref [_] conn)

      java.lang.AutoCloseable
      (close [_]
        (.close ^StatefulConnection conn)
        (.shutdown ^RedisClient client)))))

(defn add-listener!
  [conn listener]
  (us/assert! ::pubsub-connection @conn)
  (us/assert! ::pubsub-listener listener)

  (.addListener ^StatefulRedisPubSubConnection @conn
                ^RedisPubSubListener listener)
  conn)

(defn publish!
  [conn topic message]
  (us/assert! ::us/string topic)
  (us/assert! ::us/bytes message)
  (us/assert! ::connection @conn)

  (let [pcomm (.async ^StatefulRedisConnection @conn)]
    (.publish ^RedisAsyncCommands pcomm ^String topic ^bytes message)))


(defn subscribe!
  "Blocking operation, intended to be used on a worker/agent thread."
  [conn & topics]
  (us/assert! ::pubsub-connection @conn)
  (let [topics (into-array String (map str topics))
        cmd    (.sync ^StatefulRedisPubSubConnection @conn)]
    (.subscribe ^RedisPubSubCommands cmd topics)))

(defn unsubscribe!
  "Blocking operation, intended to be used on a worker/agent thread."
  [conn & topics]
  (us/assert! ::pubsub-connection @conn)
  (let [topics (into-array String (map str topics))
        cmd    (.sync ^StatefulRedisPubSubConnection @conn)]
    (.unsubscribe ^RedisPubSubCommands cmd topics)))

(defn open?
  [conn]
  (.isOpen ^StatefulConnection @conn))

(defn pubsub-listener
  [& {:keys [on-message on-subscribe on-unsubscribe]}]
  (reify RedisPubSubListener
    (message [_ pattern topic message]
      (when on-message
        (on-message pattern topic message)))

    (message [_ topic message]
      (when on-message
        (on-message nil topic message)))

    (psubscribed [_ pattern count]
      (when on-subscribe
        (on-subscribe pattern nil count)))

    (punsubscribed [_ pattern count]
      (when on-subscribe
        (on-subscribe pattern nil count)))

    (subscribed [_ topic count]
      (when on-subscribe
        (on-subscribe nil topic count)))

    (unsubscribed [_ topic count]
      (when on-subscribe
        (on-subscribe nil topic count)))))


