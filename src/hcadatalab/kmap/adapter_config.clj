(ns hcadatalab.kmap.adapter-config )

(defn wrap-timeout
  "If (f v) takes more than timeout-ms ms to compute"
  [f timeout-ms timeout-topic]
  (fn [v]
    (or (deref (future (or (f v) [])) timeout-ms nil)
      [[timeout-topic v]])))
