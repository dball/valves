(ns dball.valves
  (:require [clojure.core.async :as async
             :refer [timeout >! <! alts! go-loop close! chan >!!]])
  (:import [java.util.concurrent Executors ScheduledExecutorService TimeUnit]))

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

(defn ^ScheduledExecutorService scheduler
  []
  (Executors/newSingleThreadScheduledExecutor))

(defn clock-source
  "Creates and returns a channel that emits :tick messages at the given
   interval, in ms. If no receiver is available at the tick interval, the
   message is stored in a buffer until the next tick interval. Consumers
   that take longer than the period after consuming a tick will always
   find a tick available, however, so will tick a slower irregular rate
   dictated by their work.

   The clock uses a java.util.concurrent fixed rate scheduler, so it should
   be much more precise than timeout channels and should not accumulate error
   based on the core.async and clock overhead."
  [period-ms]
  (let [ticks (chan (async/dropping-buffer 1))
        task-fn (atom nil)
        scheduler (scheduler)
        tick-fn #(when-not (>!! ticks :tick)
                   (do
                     (.cancel @task-fn)
                     (.shutdown scheduler)))]
    (reset! task-fn (.scheduleAtFixedRate scheduler tick-fn period-ms period-ms
                                          TimeUnit/MILLISECONDS))
    ticks))

(defn flow-limiting-valve
  "Reads messages from the input and writes them to the output at a rate no
   greater than that of the given period. The rate is enforced by a clock
   source. The output channel is closed after the input channel depending on
   the close? argument, by default true."
  ([input output min-period-ms]
     (flow-limiting-valve input output min-period-ms true))
  ([input output min-period-ms close?]
     (go-loop [state :ready
               clock nil]
       (condp = state
         :ready
         (if-some [value (<! input)]
                  (do
                    (>! output value)
                    (recur :pause (clock-source min-period-ms)))
                  (recur :shutdown nil))

         :pause
         (let [[value task] (alts! [input clock] :priority true)]
           (condp = task
             input
             (if (some? value)
               (do
                 (<! clock)
                 (>! output value)
                 (recur :pause clock))
               (do
                 (close! clock)
                 (recur :shutdown nil)))

             clock
             (do
               (close! clock)
               (recur :ready nil))))

         :shutdown
         (when close? (close! output))))))

(defn typed-valve
  "Reads messages from the input and writes them to the output if they conform
   to the expected type. If a message fails to conform to the expected type,
   the output channel is closed and no more messages are read from the input."
  ([input output type-check-fn]
     (typed-valve input output type-check-fn true))
  ([input output type-check-fn close?]
     (go-loop []
       (if-some [value (<! input)]
                (if (type-check-fn value)
                  (do
                    (>! output value)
                    (recur))
                  (let [ex (ex-info "Invalid message" {:message value})]
                    (when close?
                      (close! output))
                    (throw ex)))
                (when close?
                  (close! output))))))
