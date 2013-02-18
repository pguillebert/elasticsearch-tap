(ns elasticsearchtap.elasticsearchtap
  (:import [backtype.hadoop ElasticTap ElasticScheme])
  (:use [cascalog.api])
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
