(defproject elasticsearch-tap "1.0.0"
  :description "Elsatic Search Tap for cascading"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.0"]
                 [org.apache.hadoop/hadoop-core "1.0.3"]
                 [org.elasticsearch/elasticsearch "0.19.8"]]
  :java-source-paths ["src/"]
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :aot :all
  :jvm-opts ["-Xmx1g"])
