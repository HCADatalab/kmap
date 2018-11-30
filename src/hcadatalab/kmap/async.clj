(ns hcadatalab.kmap.async
  (:require
    [clojure.core.async :as a]
    [clojure.edn :as edn]
    [gregor.core :as gregor]
    [net.cgrand.xforms :as x]
    [clj-statsd :as statsd])
  (:import
    [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer OffsetAndMetadata]
    [org.apache.kafka.clients.producer ProducerRecord KafkaProducer RecordMetadata]
    [org.apache.kafka.common TopicPartition]))

(defn- dispatch-records!
  "Takes a map of channels to [timestamp values] and attempts to send at most as possible without blocking."
  [pending-records]
  (loop [pending-records pending-records]
    (if (seq pending-records)
      (let [[_ ch] (a/alts!! (into [] (x/for [[ch [t records]] %] [ch records]) pending-records) :default nil)]
        (if (= :default ch)
          pending-records
          (recur (dissoc pending-records ch))))
      pending-records)))

(defn- topic-partition [^ConsumerRecord record]
  (TopicPartition. (.topic record) (.partition record)))

(defn- paused-partitions [pending-records]
  (into #{}
    (x/for [[t records] % record records] (topic-partition record))
    (vals pending-records)))

(defn- longest-pause-start
  "Returns the timestamp at which the oldest pause started."
  [pending-records]
  (x/some (comp (x/for [[t records] %] t) x/min) (vals pending-records)))

(defn- stateless-worker
  "f is a function from a message value to a collection of messages (a message being a tuple [topic-alias k v])
   dispatch! is a function which takes a collection as returned by f and returns a channel to which an unspecified value is written
   when all messages have been ack'ed."
  [f dispatch! get-value deps-fn error {:keys [kfn]}]
  ;; * commit can go back in time if you don't take care, commits have to be serialized
  (fn [offsets-to-commit]
    (let [deps-fn (if deps-fn
                    (fn [_ v] (deps-fn v))
                    (fn [^ConsumerRecord r _]
                      [[(.topic r) (.partition r) (.offset r)]]))
          inputs (a/chan)
          dones (a/chan 4)] ; <- param buffer?
      (a/go
        (try
          (loop [max-offsets+done nil]
            (if-some [[max-offsets done] max-offsets+done] 
              (when (some? (a/<! done))
                (a/>! offsets-to-commit max-offsets)
                (recur nil))
              (some->> (a/<! dones) recur)))
          (catch Exception e
            (error "stateless-commit" e))))
      (a/go
        (try
          (loop []
            (when-some [records (a/<! inputs)]
              (let [[max-offsets outs]
                    (x/transjuxt [(comp;max-offsets
                                    (x/by-key topic-partition
                                      (comp (map #(.offset ^ConsumerRecord %)) x/max))
                                    (x/into {}))
                                  (comp ;outs
                                    (x/for [^ConsumerRecord r %
                                            msg (f (get-value r))
                                            :when (some? msg)
                                            :let [[alias v] (if (and (sequential? msg) (= 2 (count msg)))
                                                              msg
                                                              (throw (ex-info (str "Messages should be pairs [topic-alias value], got " (pr-str msg)) {:msg msg})))
                                                  k (kfn r v)]]
                                      [alias k v (deps-fn r v)])
                                    (x/into []))]
                      records)] 
                (a/>! dones [max-offsets (dispatch! outs)]))
              (recur)))
          (catch Exception e
            (error "stateless-worker" e))))
      inputs)))

(defn wrap-traces
  ([f] (wrap-traces f identity))
  ([f summarize]
    (fn [msg]
      (let [outs (f msg)]
        (conj outs [:traces {:msg-summary (summarize msg) :out outs}])))))

(defn- dispatcher!
  "Takes a producer and a map of aliases to topic names and returns a function
   which expects a collection of tuples [alias key value] and returns a channel.
   A value is written to the channel when all messages have been acknowledged."
  [^KafkaProducer producer topic-aliases edn-out traces error]
  (fn [msgs]
    (let [to-str (if edn-out pr-str str)
          latch (a/chan 1 (comp (drop (count msgs)) (take 1)))]
      (a/>!! latch true)
      (doseq [[alias k v deps] msgs]
        (if-some [topic (topic-aliases alias)]
          (.send producer (ProducerRecord. topic k (to-str v))
            (reify org.apache.kafka.clients.producer.Callback
              (^void onCompletion [_ ^RecordMetadata metadata ^Exception ex]
                (if ex
                  (error "dispatcher" ex)
                  (do
                    (a/>!! latch true)
                    (a/>!! traces {:from deps :to [(.topic metadata) (.partition metadata) (.offset metadata)]}))))))
          (throw (RuntimeException. (str "No topic for alias " (pr-str alias) "; known mappings are: " (pr-str topic-aliases))))))
      latch)))

(def default-config {"auto.offset.reset" "latest"
                     "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                     "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                     "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                     #_#_"metric.reporters" [(class (reify org.apache.kafka.common.metrics.MetricsReporter
                                                     (configure [_ opts])
                                                     (init [_ metrics])
                                                     (metricChange [_ m]
                                                       (prn 'METRICS 'CHANGE (-> m .metricName .name) (.value m)))
                                                     (metricRemoval [_ m])
                                                     (close [_])))]})

(defn suffix-ids [config suffix]
  (-> config
    (update "client.id" str suffix)
    (update "group.id" str suffix)))

(defn metrics-progress-chan
  "Creates a channel suitable for stateful-worker :progress"
  [host port prefix delimiter period]
  (statsd/setup host port :prefix (str prefix delimiter))
  (let [ch (a/chan)
        metrics (atom {})
        scheduler (java.util.concurrent.Executors/newScheduledThreadPool 1)]
    ; TODO reset on rebalance
    (a/go-loop []
      (when-some [[topic partition tag offset] (a/<!! ch)]
        (case topic
          :reset ; in this case the msg is [:reset]
          (reset! metrics {})
          (swap! metrics assoc-in [(str topic delimiter partition) tag] offset))
        (recur)))
    ; TODO replace with periodical system
    (.scheduleAtFixedRate scheduler
      #(doseq [[topic+partition {:keys [pos end]}] @metrics :when (and end pos)]
         (statsd/gauge (str topic+partition delimiter "lag") (- end pos)))
      0
      period
      java.util.concurrent.TimeUnit/MILLISECONDS)
    ch))

(defn kafka-adapter
  "Spawns IO threads to handle kafka messages received on input-topics.
   Returns a channel, when it closes or yields a value the adapter is over.

   In stateless mode, f is a function  of message value to outs (when state-topic is nil).

   outs is a collection of pairs [alias value] representing messages to be sent.
   aliases are resolved through the topic-aliases map.

   In stateless mode, offsets are commited once all outputs are acknowledged.

   By default inputs and outputs are assumed to be edn.

   The IO thread automatically pauses and resumes upstream topics when a consumer process slows down.
   It also retrieve state from state-topic on restart.

   Options:
   :config, kafka config map, passed to producers and consumers
   :consumer-config, kafka config map, passed to consuemrs only 
     (if a property is defined here and in :config, the here value takes
     precedence as being more specific)
   :producer-config, kafka config map, same as :consumer-config but for
     producers
   :input-topics, collection of input topics (as strings)
   :timeout, integer, timeout (in ms) for poll calls, 10s (10000ms) by default
   :pause-timeout, integer, timeout (in ms) for pauses. 10min (600000ms) by default
   :edn-in/:edn-out/:edn, boolean, when true automatically parse/write/both data as edn, defaults to false
   :key-fn, key function (by default extract the key part of the record)
   :deps-fn, function that return a collection of strings identifying the dependencies of the message value (useful only when traces is set)
   :traces, chan, channel to which {:from deps :to [topic partition offset]} are written for each message produced
   :exit, chan, halts the whole adapter when closed or written to 
   :error, fn of string (part of the program) and exception, called when an exception occurs, should throw an exception
   :raw-in, boolean, the whole record is passed to the user function (instead of only the value part)
   :max-batch-size, integer, when set limits the number of records in each batch
   :worker-opts, map, passed directly to the worker see stateless worker to know more about their option
   :statsd, map (required keys :host, :port; optional keys :prefix, :delimiter, :period), stateful worker will report progress
     to statsd as key value pairs, keys will be of the form group-id.topic.partition and value the offset;
     group-id is the consumer group by default and can be configured using :prefix; :delimiter defaults to \".\"."
  [f {:keys [config consumer-config producer-config input-topics timeout pause-timeout
             edn-in edn-out edn traces deps-fn key-fn exit error
             raw-in max-batch-size worker-opts topic-aliases statsd]
      :or {timeout 10000
           pause-timeout 600000
           edn false
           worker-opts {}}}]
  (let [core-error (or error (fn [s e] (throw (RuntimeException. (str "In " s ": " (.getMessage e)) e))))
        error-exit (a/promise-chan)
        error (fn [s e]
                (a/>!! error-exit [s e]) ; can't block because error-exit is a promise-chan
                (core-error s e))
        edn-in (and (not raw-in) (if (some? edn-in) edn-in edn))
        edn-out (if(some? edn-out) edn-out edn)
        config (into default-config config)
        consumer-config (-> config (into consumer-config) (assoc "enable.auto.commit" "false"))
        producer-config (into config producer-config)
        replay-partitions (a/chan (a/sliding-buffer 1))
        to-commit (a/chan) ; offsets (stateless) or states (stateful)
        exit (or exit (a/chan))
        producer (KafkaProducer. ^java.util.Map producer-config)
        deps-fn (if traces deps-fn (constantly []))
        traces (or traces (a/chan (a/dropping-buffer 0))) ; aka /dev/null
        dispatch-outs! (dispatcher! producer topic-aliases edn-out traces error)
        progress  (if-some [{:keys [host port prefix delimiter period]
                            :or {delimiter "."
                                 prefix (consumer-config "group.id")
                                 period 1000}} statsd]
                   (metrics-progress-chan host port prefix delimiter period)
                   (a/chan (a/sliding-buffer 1)))
        worker-opts (assoc worker-opts 
                      :kfn (if key-fn
                             (fn [r v] (str (key-fn v)))
                             (fn [^ConsumerRecord r v] (.key r)))
                      :progress progress )
        get-value (cond
                    raw-in identity
                    edn-in (fn [^ConsumerRecord r] (->> r .value (edn/read-string {:default tagged-literal})))
                    :else (fn [^ConsumerRecord r] (-> r .value)))
        worker-fn stateless-worker 
        spawn-worker (worker-fn f dispatch-outs! get-value deps-fn error worker-opts)
        spawn-worker (if max-batch-size
                       (fn [& args]
                         (let [workerc (apply spawn-worker args)]
                           (doto (a/chan 1 (mapcat #(partition max-batch-size max-batch-size nil %)))
                             (a/pipe workerc))))
                       spawn-worker)
        input-thread-done
        (a/thread ; input thread
          (try
            (let [rebalanced (a/chan (a/sliding-buffer 1))
                  consumer (KafkaConsumer. ^java.util.Map consumer-config)
                  send-records (fn [pending-records tp-to-chs records]
                                 (if (vector? pending-records) ; queuing
                                   (into pending-records records)
                                   (let [now (System/currentTimeMillis)
                                         pending-records (reduce-kv
                                                           (fn [pending-records ch new-records]
                                                             (if-some [[t records] (pending-records ch)]
                                                               (assoc pending-records ch [t (into records new-records)])
                                                               (assoc pending-records ch [now new-records])))
                                                           pending-records
                                                           (group-by (comp tp-to-chs topic-partition) records))
                                         pending-records' (dispatch-records! pending-records)
                                         previously-paused (paused-partitions pending-records)
                                         to-pause (paused-partitions pending-records')
                                         pause-duration (if-some [t (longest-pause-start pending-records')]
                                                          (- now t)
                                                          0)]
                                     (when (> pause-duration pause-timeout)
                                       (throw (RuntimeException. (format "Pause exceeded max pause time of %dms" pause-duration))))
                                     (.resume consumer (into #{} (remove to-pause) previously-paused))
                                     (.pause consumer to-pause)
                                     pending-records')))
                  base-alt-ops [exit rebalanced to-commit]]
              (.subscribe consumer ^java.util.Collection input-topics
                (reify org.apache.kafka.clients.consumer.ConsumerRebalanceListener
                  (onPartitionsAssigned [_ partitions]
                    (a/>!! rebalanced partitions))
                  (onPartitionsRevoked [_ partitions] #_left-intentionally-blank)))
              (loop [pending-records [] alts-ops base-alt-ops tp-to-chs {}]
                (let [records (.poll consumer timeout)
                      [v ch] (a/alts!! alts-ops :priority true :default nil)]
                  (condp = ch
                    :default (recur (send-records pending-records tp-to-chs records) alts-ops tp-to-chs)
                    exit (doto consumer .unsubscribe .close)
                    rebalanced (let [tp-to-chs' (into {} (map (fn [^TopicPartition tp]
                                                                [tp (spawn-worker to-commit)])) v)]
                                 (doseq [ch (vals tp-to-chs)] (a/close! ch))
                                 (recur (send-records {} tp-to-chs' records) base-alt-ops tp-to-chs'))
                    to-commit ; stateless only, when stateful dealt by a separate process since consumer is not used
                    (do ; I'm worried it could queue up, to-commit should be drained
                      (.commitAsync consumer (x/into {} (x/by-key (map #(OffsetAndMetadata. %))) v) nil)
                      (recur (send-records pending-records tp-to-chs records) alts-ops tp-to-chs))
                    ; else replay-done (stateful only)
                    (let [{offsets :min-offsets-by-partition states :last-state-by-key} v]
                      (doseq [[^TopicPartition partition offset] offsets]
                        (case offset
                          :-∞
                          (.seekToBeginning consumer [partition])
                          :+∞
                          (.seekToEnd consumer [partition])
                          (.seek consumer partition (inc offset)))
                        ; don't report progress yet: we may restart from a very early position because of the conservative compitation of offsets
                        (a/>!! progress [(.topic partition) (.partition partition) :pos
                                          (case offset
                                            (:-∞ :+∞) (.position consumer partition)
                                            (inc offset))]))
                      (doseq [[key {:keys [::topic ::partition] :as state}] states]
                        (a/>!! (tp-to-chs (TopicPartition. topic partition)) state))
                      (.resume consumer (.assignment consumer))
                      ; queued records in pendirg-records are ignored as we just seeked to other positions
                      (recur {} base-alt-ops tp-to-chs))))))
            (catch Exception e
              (error "input-thread" e))))]
    (a/go (first (a/alts! [input-thread-done error-exit])))))
