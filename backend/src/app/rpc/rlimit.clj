;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.rpc.rlimit
  "Resource usage limits (in other words: semaphores)."
  (:require
   [app.common.data :as d]
   [app.common.logging :as l]
   [app.metrics :as mtx]
   [app.util.services :as sv]
   [app.rpc.rlimit.semaphore :as sem]
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


