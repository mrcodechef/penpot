;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.rpc.rlimit
  "Resource usage limits (in other words: semaphores)."
  (:require
   [app.common.data :as d]
   [app.common.data.macros :as dm]
   [app.common.logging :as l]
   [app.metrics :as mtx]
   [app.util.services :as-alias sv]
   [app.util.time :as dt]
   [app.rpc.rlimit.semaphore :as sem]
   [app.redis :as redis]
   [app.redis.script :as-alias rscript]
   [promesa.core :as p]))

(defn wrap-semaphore
  [{:keys [metrics executors] :as cfg} f mdata]
  (if-let [permits (::permits mdata)]
    (let [sem (sem/create :permits permits
                          :metrics metrics
                          :name (::sv/name mdata))]
      (l/debug :hint "wrapping rlimit" :handler (::sv/name mdata) :permits permits)
      (fn [cfg params]
        (-> (sem/acquire! sem)
            (p/then (fn [_] (f cfg params)) (:default executors))
            (p/finally (fn [_ _] (sem/release! sem))))))
    f))

(defn- timestamp
  []
  (-> (dt/now) inst-ms (/ 1000) int))

(def rate-limit-script
  {::rscript/name ::rate-limit
   ::rscript/path "app/rpc/rlimit/rlimit.lua"
   ::rscript/on-eval redis/noop-fn
   ::rscript/result-fn (fn [res]
                         [(boolean (nth res 0))
                          (nth res 1)])})

(defn redis-connect
  [state]
  (redis/connect state
                 :codec redis/string-codec
                 :timeout (dt/duration 400)))

(def ^:private default-timeout
  (dt/duration 400))

(def ^:private default-options
  {:codec redis/string-codec
   :timeout default-timeout})

(defn wrap-rlimit
  [{:keys [metrics executors redis] :as cfg} f mdata]
  (if-let [[bucket & buckets] (::buckets mdata)]
    (fn [cfg params]
      (let [conn (redis/get-or-connect redis ::rlimit default-options)]
        (-> (redis/eval! conn rate-limit-script
                         :keys [(dm/str "ratelimit." (:profile-id params))]
                         :vals [(nth bucket 0)
                                (nth bucket 1)
                                (nth bucket 2)
                                (int (/ (inst-ms (dt/now)) 1000))
                                1])
            (p/finally (fn [v e]
                         (if e (prn e) (prn v (type v)))))))
      (f cfg params))
    f))
