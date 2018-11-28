(ns hcadatalab.kmap.async-test
  (:require [clojure.test :refer [deftest is are use-fixtures testing] :as test]
    [clojure.core.async :as a]
    [clojure.edn :as edn]
    [clojure.string :as str]
    [gregor.core :as gregor]
    [net.cgrand.xforms :as x]
    [net.cgrand.xforms.rfs :as rf]
    [hcadatalab.kmap.async :refer [kafka-adapter]]
    [hcadatalab.kmap.adapter-config :as ac]
    [clj-statsd :as statsd])
  (:import
    [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer OffsetAndMetadata]
    [org.apache.kafka.clients.producer ProducerRecord KafkaProducer RecordMetadata]
    [org.apache.kafka.common TopicPartition]))

(def ^:dynamic *zk-address*)

(defn with-kafka [f]
  (let [lvl (.getLevel (org.apache.log4j.Logger/getRootLogger))]
    (.setLevel (org.apache.log4j.Logger/getRootLogger) org.apache.log4j.Level/ERROR)
    (try
      (let [zk (org.apache.curator.test.TestingServer.)
          dir (.toFile (java.nio.file.Files/createTempDirectory "kafka-adapter-test"
                         (into-array java.nio.file.attribute.FileAttribute nil)))]
      (try
        (binding [*zk-address* (str "localhost:" (.getPort zk))]
          (let [kafka 
                (-> {"broker.id" "1", "zookeeper.connect" *zk-address*
                     "log.dir" (.getCanonicalPath dir)
                     "offsets.topic.replication.factor" "1"
                     "num.partitions" "4"}
                  kafka.server.KafkaConfig.
                  kafka.server.KafkaServerStartable.
                  (doto .startup))]
            (try
              (f)
              (finally (.shutdown kafka)))))
        (finally (doto zk .stop .close))))
      (finally (.setLevel (org.apache.log4j.Logger/getRootLogger) lvl)))))

;(def statsd-metrics (atom {}))

#_(defn with-statsd-server [f]
  (let [alive (atom true)]
    (a/thread
      (let [buf (byte-array 2048)
            packet (java.net.DatagramPacket. buf (alength buf))]
        (with-open [socket (doto (java.net.DatagramSocket. 8125)
                             (.setSoTimeout 1000))]
          (while @alive
            (try
              (.receive socket packet)
              (let [[_ k v] (re-matches #"(.*?):(.*?)\|.*" (String. buf 0 (.getLength packet) "UTF-8"))]
                (swap! statsd-metrics assoc-in (str/split k #"\.") v))
              (catch java.net.SocketTimeoutException _))))))
    (f)
    (reset! alive false)
    (reset! statsd/cfg nil)))

(defn reset-clj-statsd [f]
  (reset! statsd/cfg nil)
  (f))

(use-fixtures :once with-kafka) ;with-statsd-server)
(use-fixtures :each reset-clj-statsd)

(defn nd-subtract1 [prev choice s]
  (if-some [[x & xs] (seq s)]
    (if-some [[c & cs] (seq choice)]
      (if (= c x)
        (concat (nd-subtract1 prev cs xs)
          (nd-subtract1 (conj prev c) cs s))
        (nd-subtract1 (conj prev c) cs s))
      nil)
    [(into prev choice)]))

(defn nd-subtract [choices s]
  (into #{}
    (mapcat #(nd-subtract1 [] % s))
    choices))

(defn- nd-success
  "returns true if a succesful state, false if hopelessly failed, else nil."
  [choices]
  (if (seq choices)
    (if (choices []) true nil)
    false))

(defn- nd-comsume [nd-expected-outcomes records]
  (let [partitioned-msgs
        (into []
          (comp
            (x/for [^ConsumerRecord r %]
              [[(.topic r) (.partition r)] [(.topic r) (edn/read-string (.value r))]])
            (x/by-key (x/into []))
            x/vals)
          records)]
    (reduce nd-subtract nd-expected-outcomes partitioned-msgs)))

(defn- nd
  ([topic expected-outcomes] (nd {topic expected-outcomes}))
  ([expected-outcomes-per-topic]
    #{(into [] (for [[topic vs] expected-outcomes-per-topic
                     v vs]
                 [topic v]))}))

(deftest stateless
  (testing "Stateless worker"
    (let [input-topic (str "test-stateless-in" (System/currentTimeMillis))
         output-topic (str "test-stateless-out" (System/currentTimeMillis))
         N 100 ;msgs
         f #(inc (* 2 %))
         exit (a/chan)
         adapter
         (kafka-adapter
           (fn [v] [[:out (f v)]])
           {:config {"group.id" "stateless"
                     "bootstrap.servers" "localhost:9092"
                     "auto.offset.reset" "earliest"
                     "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
            :timeout 1000
            :edn true
            :exit exit
            :input-topics [input-topic]
            :topic-aliases {:out output-topic}})]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range N)]
           (Thread/sleep 50)
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
     (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-stateless"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
       (loop [nd-expected-outcomes (nd output-topic (map f (range N)))]
         (let [nd-expected-outcomes (nd-comsume nd-expected-outcomes (.poll consumer 1000))]
           (case (nd-success nd-expected-outcomes)
             nil (recur nd-expected-outcomes)
             true (is true)
             false (is false)))))
     (a/close! exit)
     (a/<!! adapter))))

(deftest stateless-1-by-1
  (testing "Stateless worker with batches of 1 item"
    (let [input-topic (str "test-stateless-1-by-1-in" (System/currentTimeMillis))
          output-topic (str "test-stateless-1-by-1-out" (System/currentTimeMillis))
          N 100 ;msgs
          f #(inc (* 2 %))
          exit (a/chan)
          adapter
          (kafka-adapter
            (fn [v] [[:out (f v)]])
            {:config {"group.id" "stateless-1-by-1"
                      "bootstrap.servers" "localhost:9092"
                      "auto.offset.reset" "earliest"
                      "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
             :max-batch-size 1
             :timeout 1000
             :edn true
             :exit exit
             :input-topics [input-topic]
             :topic-aliases {:out output-topic}})]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range N)]
           (Thread/sleep 50)
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
     (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-stateless-1-by-1"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
       (loop [nd-expected-outcomes (nd output-topic (map f (range N)))]
         (let [nd-expected-outcomes (nd-comsume nd-expected-outcomes (.poll consumer 1000))]
           (case (nd-success nd-expected-outcomes)
             nil (recur nd-expected-outcomes)
             true (is true)
             false (is false)))))
     (a/close! exit)
     (a/<!! adapter))))

(deftest bad-alias
  (testing "Unmapped topic alias should crash"
    (let [input-topic (str "test-bad-alias-in" (System/currentTimeMillis))
         output-topic (str "test-bad-alias-out" (System/currentTimeMillis))
         N 100 ;msgs
         f #(inc (* 2 %))
         exit (a/chan)
         error (a/chan)
         adapter
         (kafka-adapter
           (fn [v] [[:out (f v)]])
           {:config {"group.id" "bad-alias"
                     "bootstrap.servers" "localhost:9092"
                     "auto.offset.reset" "earliest"
                     "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
            :timeout 1000
            :edn true
            :exit exit
            :input-topics [input-topic]
            :topic-aliases {:should-be-out output-topic}
            :error (fn [s e] (a/>!! error [s e]))})]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range N)]
           (Thread/sleep 50)
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
     (is (= "stateless-worker" (first (a/<!! error))))
     (is (some? (a/<!! adapter)))))) ; the error causes the adapter to exit

(deftest stateless-filter
  (testing "Stateless worker which filters out some inputs"
    (let [input-topic (str "test-stateless-in" (System/currentTimeMillis))
         output-topic (str "test-stateless-out" (System/currentTimeMillis))
         N 100 ;msgs
         exit (a/chan)
         adapter
         (kafka-adapter
           (fn [v] (when (odd? v) [[:out v]]))
           {:config {"group.id" "stateless-filter"
                     "bootstrap.servers" "localhost:9092"
                     "auto.offset.reset" "earliest"
                     "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
            :timeout 1000
            :edn true
            :exit exit
            :input-topics [input-topic]
            :topic-aliases {:out output-topic}})]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range N)]
           (Thread/sleep 50)
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
     (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-stateless-filter"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
       (loop [nd-expected-outcomes (nd output-topic (filter odd? (range N)))]
         (let [nd-expected-outcomes (nd-comsume nd-expected-outcomes (.poll consumer 1000))]
           (case (nd-success nd-expected-outcomes)
             nil (recur nd-expected-outcomes)
             true (is true)
             false (is false)))))
     (a/close! exit)
     (a/<!! adapter))))

(defn consume-expected [expected msgs]
  (let [expected
        (into expected
          (x/for [[k ms] %
                  :let [n (count ms)
                        exs (expected k)]]
            (do
              (is (= (take n exs) ms))
              [k (drop n exs)]))
          msgs)]
    (when (or (seq msgs) (seq (mapcat val expected)))
      expected)))

;(def statsd-period 200)

#_(deftest stateful
  (testing "Stateful worker"
    (let [input-topic (str "test-stateful-in" (System/currentTimeMillis))
         output-topic (str "test-stateful-out" (System/currentTimeMillis))
         state-topic (str "test-stateful-state" (System/currentTimeMillis))
         N 100 ;msgs
         running-avg
         (fn 
           ([] #{}) ; assumes no repeated values
           ([ns] ns)
           ([ns n]
             (let [ns (conj ns n)]
               [ns [(/ (reduce + ns) (count ns))]])))
         exit (a/chan)
         start-adapter
         #(kafka-adapter running-avg
           {:config {"group.id" "stateful"
                     "bootstrap.servers" "localhost:9092"
                     "auto.offset.reset" "earliest"
                     "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
            :timeout 1000
            :edn true
            :exit exit
            :input-topics [input-topic]
            :topic-aliases {:state state-topic
                            :out output-topic}
            :statsd {:host "localhost" :port 8125 :period statsd-period}})
         adapter (atom (start-adapter))]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range (/ N 2))]
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))
         (.flush p)
         (Thread/sleep 50000)
         (a/>!! exit :shutdown)
         (a/<!! @adapter)
         #_(Thread/sleep 10000)
         (reset! adapter (start-adapter))
         (doseq [n (range (/ N 2) N)]
           (Thread/sleep 50)
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))
         (.flush p)))
     (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-stateful"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
       (loop [expected (x/into {}
                         (x/by-key #(str "P" (mod % 5))
                           (comp (x/reductions conj [])
                             (drop 1)
                             (map (fn [ns] (/ (reduce + ns) (count ns))))
                             (x/into [])))
                         (range N))
              already-received (zipmap (keys expected) (repeat #{}))]
         (let [msgs (into {}
                      (x/by-key #(.key %)
                        (comp
                          (x/for [r %
                                  :let [v (edn/read-string (.value r))
                                        k (.key r)]
                                  :when (not ((already-received k) v))]
                            v)
                          (x/into [])))
                      (.poll consumer 1000))]
           (some-> expected (consume-expected msgs) (recur (merge-with into already-received msgs))))))
     (a/close! exit)
     (a/<!! @adapter)
     (Thread/sleep (* 2 statsd-period)) ; Nyquist–Shannon sampling theorem :-)
     (is (= "0" (get-in @statsd-metrics ["stateful" input-topic "0" "lag"]))))))

(deftest stall
  (testing "Stall detection"
    (let [input-topic (str "test-stall-in" (System/currentTimeMillis))
         output-topic (str "test-stall-out" (System/currentTimeMillis))
         stalled-topic (str "test-stall-stalled" (System/currentTimeMillis))
         N 3 ;msgs
         exit (a/chan)
         adapter
         (kafka-adapter
           (ac/wrap-timeout
             (fn [v] 
               (when (= v 1) (Thread/sleep 10000))
               [[:out (- v)]])
             1000 :timeout)
           {:config {"group.id" "stall"
                     "bootstrap.servers" "localhost:9092"
                     "auto.offset.reset" "earliest"
                     "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
            :timeout 1000
            :edn true
            :exit exit
            :input-topics [input-topic]
            :topic-aliases {:out output-topic
                            :timeout stalled-topic}})]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range N)]
           (Thread/sleep 50)
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
     
     (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-stall"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic stalled-topic]))]
       (loop [nd-expected-outcomes (nd {output-topic [0 -2]
                                        stalled-topic [1]})]
         (let [nd-expected-outcomes (nd-comsume nd-expected-outcomes (.poll consumer 1000))]
           (case (nd-success nd-expected-outcomes)
             nil (recur nd-expected-outcomes)
             true (is true)
             false (is false)))))
     (a/close! exit)
     (a/<!! adapter))))

(deftest rekeying
  (let [input-topic (str "test-rekeying-in" (System/currentTimeMillis))
        output-topic (str "test-rekeying-out" (System/currentTimeMillis))
        N 20 ;msgs
        exit (a/chan)
        adapter
        (kafka-adapter
          (fn [v] [[:out (inc v)]])
          {:config {"group.id" "rekeying"
                    "bootstrap.servers" "localhost:9092"
                    "auto.offset.reset" "earliest"
                    "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
           :timeout 1000
           :edn true
           :exit exit
           :key-fn (fn [n] (mod n 4))
           :input-topics [input-topic]
           :topic-aliases {:out output-topic}})]
    (a/thread
      (with-open [p (gregor/producer "localhost:9092")]
        (doseq [n (range N)]
          (Thread/sleep 50)
          (gregor/send p input-topic (pr-str n)))))
    (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                 "dumb-rekeying"
                                 [] {"auto.offset.reset" "earliest"
                                     "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                           (.subscribe [output-topic]))]
      (loop [expected (x/into {} (comp (map inc) (x/by-key #(pr-str (mod % 4)) (x/into []))) (range N))]
        (let [msgs (into {} (x/by-key #(.key %) #(edn/read-string (.value %)) (x/into [])) (.poll consumer 1000))]
          (some-> expected (consume-expected msgs) recur))))
    (a/close! exit)
    (a/<!! adapter)))

(defn consume-partially-expected [expected msgs]
  (let [expected
        (into expected
          (x/for [[k ms] %
                  :let [n (count ms)
                        exs (expected k)]]
            (do
              (doseq [[expected actual] (map (fn [a b]
                                               [(take-last (count b) a) b])  exs ms)]
                (is (= expected actual)))
              [k (drop n exs)]))
          msgs)]
    (when (or (seq msgs) (seq (mapcat val expected)))
      expected)))

#_(deftest cleanup
  (let [input-topic (str "test-cleanup-in" (System/currentTimeMillis))
        output-topic (str "test-cleanup-out" (System/currentTimeMillis))
        state-topic (str "test-cleanup-state" (System/currentTimeMillis))
        N 100 ;msgs
        accumulate
        (fn 
          ([] [])
          ([acc] acc)
          ([acc n]
            [(conj acc n) [n]]))
        exit (a/chan)
        adapter
        (kafka-adapter accumulate
          {:config {"group.id" "cleanup"
                    "bootstrap.servers" "localhost:9092"
                    "auto.offset.reset" "earliest"
                    "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
           :timeout 1000
           :edn true
           :exit exit
           :worker-opts {:summarize x/max
                         :cleanup (fn [state max]
                                    (into [] (filter #(<= (- max 20) %)) state))}
           :input-topics [input-topic]
           :topic-aliases {:state state-topic
                           :out output-topic}
           :statsd {:host "localhost" :port 8125 :period statsd-period}})]
    (with-open [p (gregor/producer "localhost:9092")]
      (doseq [n (range N)]
        (Thread/sleep 200)
        (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))
      (.flush p))
    ; wait for complete execution
    (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                 "dumb-cleanup"
                                 [] {"auto.offset.reset" "earliest"
                                     "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                           (.subscribe [output-topic]))]
      (loop [ns (set (range N))]
        (when (seq ns)
          (recur (transduce (map #(-> % .value edn/read-string)) disj ns (.poll consumer 1000))))))
    (Thread/sleep (* 2 statsd-period)) ; Nyquist–Shannon sampling theorem :-)
    (a/close! exit)
    (a/<!! adapter)
    (is (= "0" (get-in @statsd-metrics ["cleanup" input-topic "0" "lag"])))
    (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                 "dumb-cleanup"
                                 [] {"auto.offset.reset" "earliest"
                                     "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                           (.subscribe [state-topic]))]
      (loop [greatest -1]
        (when (< greatest (dec N))
          (let [states (into [] (keep #(:hcadatalab.kmap.async/value (edn/read-string (.value %)))) (.poll consumer 1000))]
            (doseq [state states]
              (is (= (sort state) state))
              (is (<= (- (peek state) (first state)) 20)))
            (recur (transduce (map peek) max greatest states))))))))

#_(deftest killing-bg-thread
    (testing "Ensuring that an exception thrown in a callback can't bring down the producer."
      (let [input-topic (str "test-killing-bg-thread" (System/currentTimeMillis))
            thrown (promise)
            done (promise)]
        (with-open [p (gregor/producer "localhost:9092")]
          (.send p (ProducerRecord. input-topic "foo" "bar")
            (reify org.apache.kafka.clients.producer.Callback
              (^void onCompletion [_ ^RecordMetadata metadata ^Exception ex]
                (deliver thrown true)
                (throw (ex-info "Die, bg thread, die!" {})))))
          (is (= true (deref thrown 10000 false)))
          (Thread/sleep 1000)
          (.send p (ProducerRecord. input-topic "foo" "baz")
            (reify org.apache.kafka.clients.producer.Callback
              (^void onCompletion [_ ^RecordMetadata metadata ^Exception ex]
                (deliver done true))))
          (is (= true (deref done 10000 false)))))))

(deftest slow-stateless
  (testing "Stateless worker"
    (let [input-topic (str "test-slow-stateless-in" (System/currentTimeMillis))
         output-topic (str "test-slow-stateless-out" (System/currentTimeMillis))
         N 100 ;msgs
         f #(do
              (Thread/sleep 200) ; so can only process 5 msg/s
              (inc (* 2 %)))
         exit (a/chan)
         adapter
         (kafka-adapter
           (fn [v] [[:out (f v)]])
           {:config {"group.id" "stateless"
                     "bootstrap.servers" "localhost:9092"
                     "auto.offset.reset" "earliest"
                     "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
            :timeout 100 ; should get around 2 msg for each poll
            :edn true
            :exit exit
            :input-topics [input-topic]
            :topic-aliases {:out output-topic}})]
     (a/thread
       (with-open [p (gregor/producer "localhost:9092")]
         (doseq [n (range N)]
           (Thread/sleep 50) ; production rate is 20 msg/s
           (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
     (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-slow-stateless"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
       (loop [nd-expected-outcomes (nd output-topic (map f (range N)))]
         (let [nd-expected-outcomes (nd-comsume nd-expected-outcomes (.poll consumer 1000))]
           (case (nd-success nd-expected-outcomes)
             nil (recur nd-expected-outcomes)
             true (is true)
             false (is false)))))
     (a/close! exit)
     (a/<!! adapter))))

#_(deftest failing-stateless
   (testing "Stateless worker"
     (let [input-topic (str "test-failing-stateless-in" (System/currentTimeMillis))
          output-topic (str "test-failing-stateless-out" (System/currentTimeMillis))
          N 100 ;msgs
          f #(if (< % 50) (inc (* 2 %)) (throw (ex-info "Oops" {})))
          exit (a/chan)
          adapter
          (kafka-adapter
            (fn [v] [[:out (f v)]])
            {:config {"group.id" "stateless"
                      "bootstrap.servers" "localhost:9092"
                      "auto.offset.reset" "earliest"
                      "metadata.max.age.ms" "10000"} ; to synchronize properly for testing
             :timeout 1000
             :edn true
             :exit exit
             :input-topics [input-topic]
             :topic-aliases {:out output-topic}})]
      (a/thread
        (with-open [p (gregor/producer "localhost:9092")]
          (doseq [n (range N)]
            (Thread/sleep 50)
            (gregor/send p input-topic (str "P" (mod n 5)) (pr-str n)))))
      (with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-failing-stateless"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
        (loop [expected (map f (range N))]
          (let [msgs (into [] (map #(edn/read-string (.value %))) (.poll consumer 1000))
                n (count msgs)]
            (is (= (take n expected) msgs))
            (when (or (pos? n) (seq expected))
              (recur (drop n expected))))))
      (a/close! exit)
      (a/<!! adapter))))
