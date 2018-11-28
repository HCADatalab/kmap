(ns hcadatalab.kmap.async-fail-test
  (:require [clojure.test :refer [deftest is are use-fixtures testing] :as test]
    [clojure.core.async :as a]
    [clojure.edn :as edn]
    [gregor.core :as gregor]
    [net.cgrand.xforms :as x]
    [net.cgrand.xforms.rfs :as rf]
    [hcadatalab.kmap.async :refer [kafka-adapter]]
    [hcadatalab.kmap.adapter-config :as ac])
  (:import
    [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer OffsetAndMetadata]
    [org.apache.kafka.clients.producer ProducerRecord KafkaProducer RecordMetadata]
    [org.apache.kafka.common TopicPartition]))

(def ^:dynamic *zk-address*)

(defn with-zk [f]
  (let [lvl (.getLevel (org.apache.log4j.Logger/getRootLogger))]
    (.setLevel (org.apache.log4j.Logger/getRootLogger) org.apache.log4j.Level/ERROR)
    (try
      (let [zk (org.apache.curator.test.TestingServer.)]
        (try
          (binding [*zk-address* (str "localhost:" (.getPort zk))]
            (f))
          (finally (doto zk .stop .close))))
      (finally (.setLevel (org.apache.log4j.Logger/getRootLogger) lvl)))))

(use-fixtures :once with-zk)

(defn spin-broker [nick]
  (let [dir (.toFile (java.nio.file.Files/createTempDirectory (str "kafka-" nick)
                       (into-array java.nio.file.attribute.FileAttribute nil)))]
    (-> {"broker.id" "1", "zookeeper.connect" *zk-address*
         "log.dir" (.getCanonicalPath dir)
         "offsets.topic.replication.factor" "1"}
      kafka.server.KafkaConfig.
      kafka.server.KafkaServerStartable.
      (doto .startup))))

(deftest server-failure
  (let [input-topic (str "test-brokerfailure-in" (System/currentTimeMillis))
        output-topic (str "test-brokerfailure-out" (System/currentTimeMillis))
        kafka (spin-broker "failure-test")
        #_(finally (.shutdown kafka) (.awaitShutdown kafka))
        N 100 ;msgs
        f #(inc (* 2 %))
        exit (a/chan)
        adapter
        (kafka-adapter
          (fn [v] (when (= v 30)
                    #_(/ 0)
                    (.shutdown kafka) (.awaitShutdown kafka)) [[:out (f v)]])
          {:config {"group.id" "brokerfailure"
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
    #_(with-open [consumer (doto (gregor/consumer "localhost:9092"
                                  "dumb-brokerfailure"
                                  [] {"auto.offset.reset" "earliest"
                                      "metadata.max.age.ms" "10000"}) ; to synchronize properly for testing
                            (.subscribe [output-topic]))]
       (loop [expected (map f (range N))]
         (let [msgs (into [] (map #(edn/read-string (.value %))) (.poll consumer 1000))
               n (count msgs)]
           (is (= (take n expected) msgs))
           (when (or (pos? n) (seq expected))
             (recur (drop n expected))))))
    (let [[source ex] (a/<!! adapter)]
      (is (instance? Exception ex)))))

