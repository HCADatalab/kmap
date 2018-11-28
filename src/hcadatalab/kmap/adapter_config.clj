(ns hcadatalab.kmap.adapter-config )

#_(defn wrap-error-topic
  "Catches otherwise uncaught errors in the fn and returns the message that
  caused the error and relevant error info to the error topic.
  This fn requires the user to define the alias for :error-topic"
  [f]
  (fn [v]
    (try
      (f v)
      (catch Exception e
        (do
          (error :msg "Exception while processing message"
                 :v v
                 :exception e)
          [[:error-topic #_(str (UUID/randomUUID))              ;TODO:should be message key
           #_(pr-str )
           {:v v :e (io.aviso.exception/analyze-exception e {})}]])))))

#_(defn wrap-debug-logger
  "At debug log level returns the input and output of processing a message"
  [f]
  (fn [v]
    (let [res (f v)]
      (do
        (debug :v v :res res)
        res))))

(defn wrap-timeout
  "If (f v) takes more than timeout-ms ms to compute"
  [f timeout-ms timeout-topic]
  (fn [v]
    (or (deref (future (or (f v) [])) timeout-ms nil)
      [[timeout-topic v]])))
