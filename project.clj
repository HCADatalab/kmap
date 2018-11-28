(defproject hcadatalab/kmap "1.0.0"
  :description "Tools for kafka services" 
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
    :dependencies [[org.clojure/clojure "1.9.0"]
                 [io.weft/gregor "0.6.0"] ; kafka 0.11
                 [org.clojure/core.async "0.3.443"]
                 [net.cgrand/xforms "0.16.0"]
                 [clj-statsd "0.4.0"]
                 [io.aviso/pretty     "0.1.33"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :test {:plugins [[lein-test-report-junit-xml "0.2.0"]]
                    :dependencies [[org.slf4j/slf4j-log4j12 "1.6.2"]
                                   [log4j "1.2.16"]]}
             :dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                  [org.apache.curator/curator-test "2.8.0"]] }})

