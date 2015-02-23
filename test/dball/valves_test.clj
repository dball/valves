(ns dball.valves-test
  (:require [clojure.core.async :refer [chan >!! <!! close! go-loop <!] :as async]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [dball.valves :refer :all]))

(defn now-ms
  []
  (System/currentTimeMillis))

(defn build-schedule
  [total]
  (let [pause-ms (gen/choose 0 100)]
    (gen/vector pause-ms total)))

(defn run-schedule
  [input schedule]
  (let [times (mapv (fn [ms]
                      (Thread/sleep ms)
                      (let [now (now-ms)]
                        (>!! input now)
                        now))
                    schedule)]
    (close! input)
    times))

(defn timed-into
  [coll ch]
  (go-loop [coll coll]
    (let [value (<! ch)]
      (if (some? value)
        (recur (conj coll [(now-ms) value]))
        coll))))

(def timeout-error-ms
  clojure.core.async.impl.timers/TIMEOUT_RESOLUTION_MS)

(defspec test-batching-valve
  100
  (prop/for-all [max-count (gen/frequency [[9 (gen/choose 2 10)] [1 (gen/return nil)]])
                 max-ms (gen/choose 10 500)
                 schedule (gen/bind (gen/choose 0 100) build-schedule)]
    (let [input (chan)
          output (chan)
          valve (batching-valve input output max-ms max-count)
          timed-batches (timed-into [] output)
          messages (run-schedule input schedule)
          timed-batches (<!! timed-batches)
          batches (map last timed-batches)
          batch-times (map (fn [[time batch]] (- time (first batch))) timed-batches)]
      (and
       ;; all messages are conveyed
       (= messages (apply concat batches))

       ;; batches are never empty or larger than the max-count
       (every? (fn [batch]
                 (and (seq batch)
                      (or (not max-count)
                          (>= max-count (count batch)))))
               batches)

       ;; batches never take longer than max-ms + the timeout channel
       ;; imprecision factor
       (every? (fn [batch-time]
                 (let [delta (- max-ms batch-time)]
                   (pos? (+ delta timeout-error-ms))))
               batch-times)))))
