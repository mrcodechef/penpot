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
   [app.config :as cf]
   [app.metrics :as mtx]
   [app.redis :as redis]
   [app.redis.script :as-alias rscript]
   [app.rpc.rlimit.semaphore :as rsem]
   [app.rpc.rlimit.buckets :as-alias rbucket]
   [app.util.services :as-alias sv]
   [app.util.time :as dt]
   [cuerdas.core :as str]
   [promesa.core :as p]))

(defn wrap-semaphore
  [{:keys [metrics executors] :as cfg} f mdata]
  (if-let [permits (::permits mdata)]
    (let [sem (rsem/create :permits permits
                           :metrics metrics
                           :name (::sv/name mdata))]
      (l/debug :hint "wrapping rlimit" :handler (::sv/name mdata) :permits permits)
      (fn [cfg params]
        (-> (rsem/acquire! sem)
            (p/then (fn [_] (f cfg params)) (:default executors))
            (p/finally (fn [_ _] (rsem/release! sem))))))
    f))

(def ^:private rate-limit-script
  {::rscript/name ::rate-limit
   ::rscript/path "app/rpc/rlimit/rlimit.lua"
   ::rscript/result-fn identity})

(def ^:private default-timeout
  (dt/duration 400))

(def ^:private default-options
  {:codec redis/string-codec
   :timeout default-timeout})

(defn- bucket->str
  [bucket]
  (str/join "." bucket))

(defn- prepare-bucket
  [sname {:keys [interval rate capacity] :as bb}]
  (prn "prepare-bucket" sname bb)
  (let [interval (-> interval dt/duration inst-ms (/ 1000) int)
        repr (dm/str "[i:" interval " r:" rate " c:" capacity "]")]
    {::rbucket/name repr
     ::rbucket/params [interval rate capacity]
     ::rbucket/key (dm/str "ratelimit." interval "." rate "." capacity "." sname)}))

(defn wrap-rlimit
  [{:keys [metrics executors redis] :as cfg} f mdata]
  (letfn [(process-bucket [conn profile-id now {:keys [::rbucket/key ::rbucket/params] :as bucket}]
            (-> (redis/eval! conn rate-limit-script
                             :keys [(dm/str key "." profile-id)]
                             :vals (conj params now))
                (p/then (fn [result]
                          (let [allowed? (boolean (nth result 0))
                                remaining (nth result 1)]
                            (l/trace :hint "rate limit processed"
                                     :service (::sv/name mdata)
                                     :bucket (::rbucket/name bucket)
                                     :allowed? allowed?
                                     :remaining remaining)
                            {:allowed? allowed?
                             :remaining remaining
                             :bucket (::rbucket/name bucket)})))))

          (process-buckets [conn profile-id buckets]
            (let [now (-> (dt/now) inst-ms (/ 1000) int)]
              (-> (p/all (map (partial process-bucket conn profile-id now) (reverse buckets)))
                  (p/then (fn [results]
                            (prn "KKK" results)
                            (or (d/seek (complement :allowed?) results)
                                (first results)))))))

          (on-error [cause]
            (l/error :hint "error on processing rate-limit" :cause cause))]

    (let [sname   (::sv/name mdata)
          buckets (map (partial prepare-bucket sname)
                       (::buckets mdata))]
      (if (seq buckets)
        (fn [cfg {:keys [profile-id] :as params}]
          (let [result (if (and profile-id (contains? cf/flags :rate-limit))
                         (let [conn (redis/get-or-connect redis ::rlimit default-options)]
                           (-> (process-buckets conn profile-id buckets)
                               (p/catch on-error)))
                         (p/resolved {}))]
            (f cfg params)))
        f))))


  ;; (if-let [[bucket & buckets] (::buckets mdata)]
  ;;   (fn [cfg params]
  ;;     (let [conn (redis/get-or-connect redis ::rlimit default-options)]
  ;;       (-> (redis/eval! conn rate-limit-script
  ;;                        :keys [(dm/str "ratelimit." (:profile-id params))]
  ;;                        :vals (into bucket [(int (/ (inst-ms (dt/now)) 1000)) 1]))
  ;;           (p/finally (fn [v e]
  ;;                        (if e (prn e) (prn v (type v)))))))
  ;;     (f cfg params))
  ;;   f))
