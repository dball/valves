(ns dball.valves
  (:require [clojure.core.async :refer [timeout >! <! alts! go-loop close!]]))

(defn batching-valve
  "Reads messages from the input, stores them in batches, and writes the
   batches to the output when either the batch has enough items or enough
   time has elapsed since receiving the first message in the batch.

   The batches are vectors and will never be empty or contain more than
   the maximum number of items, if given. The time interval must be given
   in milliseconds, and is only as precise as timeout channels."
  [input output max-ms max-count]
  (go-loop [batch nil
            timer nil]
    (assert (or (and timer batch)
                (and (not timer) (not batch))))
    (if-not batch
      (let [value (<! input)]
        (if (some? value)
          (recur [value] (timeout max-ms))
          (close! output)))
      (let [[value task] (alts! [timer input] :priority true)]
        (cond (= timer task)
              (do
                (>! output batch)
                (recur nil nil))

              (some? value)
              (let [batch (conj batch value)]
                (if (and max-count (= max-count (count batch)))
                  (do
                    (>! output batch)
                    (recur nil nil))
                  (recur batch timer)))

              :else
              (do
                (>! output batch)
                (close! output)))))))
