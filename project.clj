(defproject elasticsearch-tap "1.0.0"
  :description "Elastic Search Tap for cascading"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.1" :exclusions [[cascalog/cascalog-elephantdb]
                                                 [cascalog/midje-cascalog]
                                                 [org.clojure/clojure]]]
                 [org.apache.hadoop/hadoop-core "1.0.3"]
                 [clj-time "0.4.4"]
                 [org.elasticsearch/elasticsearch "0.19.8"]]
  :java-source-paths ["src/"]
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :aot :all
  :jvm-opts ["-Xmx1g"])
