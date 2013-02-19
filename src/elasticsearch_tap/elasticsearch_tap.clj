(ns elasticsearch-tap.elasticsearch-tap
  (:import [backtype.hadoop ElasticTap ElasticScheme])
  (:use [cascalog.api])
  (:require [clj-time [core :as time] [format :as time-fmt] [coerce :as time-coerce]])
  (:gen-class))

(def sample-es-conf
  {"elasticsearch.index.name" "contents"
   "elasticsearch.object.type" "content"
   "elasticsearch.request.size" 100
   "elasticsearch.num.input.splits" 10
   "elasticsearch.query.string" " {
 filtered : {
             query : { match_all : {}   },
             filter : {    and : [  {  exists : { field : \"content_id\"   } },
                                    { range : { publication_date : {from : \"2012-10-25T00:00:00Z\",
                                                                    to : \"2012-10-28T23:59:00Z\" } } }
                                    ]
                       }
             }
 }
"
   "elasticsearch.cluster.hosts" "ymes1.youmag.com"
   "elasticsearch.cluster.name" "youmag"
   "elasticsearch.transport.port" 9300
   "elasticsearch.transport.timeout" "60s"
   "mapred.input.dir" "file:///dev/null"
   })

(defn sample-query
  [job-conf]
  (let [estap (ElasticTap.)]
    (with-job-conf job-conf
      (?<- (stdout) [?id ?json]
           (estap ?id ?json)))))

(def sample-template
  {"elasticsearch.index.name" "contents"
   "elasticsearch.object.type" "content"
   "elasticsearch.request.size" 100
   "elasticsearch.num.input.splits" 10
   "elasticsearch.query.string" "{
 filtered : {
             query : { text : { _all : \"montpellier\" }   },
             filter : { and : [  {  exists : { field : \"content_id\"   } },
                                 {  range : { publication_date : {from : %%FROM%% ,
                                                                  to : %%TO%%  } } }
                                 ]
                       }
             }
 }"
   "elasticsearch.cluster.hosts" "ymes1.youmag.com"
   "elasticsearch.cluster.name" "youmag"
   "elasticsearch.transport.port" 9300
   "elasticsearch.transport.timeout" "60s"
   "mapred.input.dir" "file:///dev/null"
   })


;;;;;;;;;;;;;;;;;;;;;;;
;;;;; Elastic Tap ;;;;;
;;;;;;;;;;;;;;;;;;;;;;;

(defn get-jobconf-from-template
  [template conf]
  (let [query-str (get template "elasticsearch.query.string")
        cluster-hosts (get template "elasticsearch.cluster.hosts")
        from (:from conf)
        to (:to conf)
        server (:server conf)
        now (time/now)
        y (time/year now)
        m (time/month now)
        d (time/day now)
        today-at-midnight (time/date-time y m d)
        from-date  (time/minus today-at-midnight (time/days from))
        to-date    (time/minus today-at-midnight (time/days to))

        from (str "\"" (time-fmt/unparse (time-fmt/formatters :date-time-no-ms) from-date) "\"")  ;; "\"2012-10-25T00:00:00Z\""
        to   (str "\"" (time-fmt/unparse (time-fmt/formatters :date-time-no-ms) to-date) "\"") ;; "\"2012-10-28T23:59:00Z\""

        query-str (.replaceAll query-str "%%FROM%%" from)
        query-str (.replaceAll query-str "%%TO%%" to)

        cluster-hosts (.replaceAll cluster-hosts "%%SERVER%%" server)]

    (assoc template "elasticsearch.query.string" query-str
           "elasticsearch.cluster.hosts" cluster-hosts)))
