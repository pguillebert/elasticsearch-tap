package backtype.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Hadoop InputFormat to read data from an Elasticsearch index. The RecordReader divulges records
 * where the key is the record id in elasticsearch and the value is a json string of the (source)
 * record contents.
 */
public class ElasticSearchInputFormat implements Configurable, InputFormat<Text, Text> {
	static Log LOG = LogFactory.getLog(ElasticSearchInputFormat.class);
    private Configuration conf = null;
    private Client client;
    private Long numHits;
    private Long numSplits;
    private Long numSplitRecords;
    private String indexName;
    private String objType;
    private String queryString;
    private String[] hosts;
    private String clusterName;
	private int port;

    private static final String ES_NUM_SPLITS = "elasticsearch.num.input.splits";
    private static final String ES_QUERY_STRING = "elasticsearch.query.string";
    private static final String ES_CLUSTER_NAME = "elasticsearch.cluster.name";
    private static final String ES_CLUSTER_HOSTS = "elasticsearch.cluster.hosts";
    private static final String ES_TRANSPORT_PORT = "elasticsearch.transport.port";
    private static final String ES_INDEX_NAME = "elasticsearch.index.name";
    private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
	private static final String ES_TRANSPORT_TIMEOUT = "elasticsearch.transport.timeout";

	private ElasticSearchRecordReader singleton;
	private String connect_timeout;
 
	public RecordReader<Text, Text> getRecordReader(InputSplit is, JobConf jc, Reporter reporter) {
		LOG.info("ElasticSearchInputFormat: getRecordReader");
		if(singleton == null) {
			this.singleton = new ElasticSearchRecordReader(is);
		}
		return singleton;
	}

	/** The number of splits is specified in the Hadoop configuration object. */
	public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
		LOG.info("ElasticSearchInputFormat: getSplits...");
		
		setConf(conf);

        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits.intValue());

        for (int i = 0; i < numSplits; i++) {
            Long size = (numSplitRecords == 1) ? 1 : numSplitRecords - 1;
            splits.add(new ElasticSearchSplit(queryString, i * numSplitRecords, size));
        }
        if (numHits % numSplits > 0) {
            splits.add(new ElasticSearchSplit(queryString,
                numSplits * numSplitRecords, numHits % numSplits - 1));
        }
        LOG.info("Created [" + splits.size() + "] splits for [" + numHits + "] hits");

        return splits.toArray(new InputSplit[splits.size()]);
    }

    /**
     * Sets the configuration object, opens a connection to elasticsearch, and initiates the initial
     * search request.
     */
    public void setConf(Configuration configuration) {
        LOG.info("ElasticSearchInputFormat: setConf on hosts "+ configuration.get(ES_CLUSTER_HOSTS));

        this.conf = configuration;
        this.indexName = conf.get(ES_INDEX_NAME);
        this.objType = conf.get(ES_OBJECT_TYPE);
        this.numSplits = Long.parseLong(conf.get(ES_NUM_SPLITS));
        this.queryString = conf.get(ES_QUERY_STRING);
        this.clusterName = conf.get(ES_CLUSTER_NAME);
        String tmpValue = conf.get(ES_CLUSTER_HOSTS);
    	this.hosts = tmpValue.split("\\s*[,:|]\\s*");
    	this.port = Integer.parseInt(conf.get(ES_TRANSPORT_PORT));
    	this.connect_timeout = conf.get(ES_TRANSPORT_TIMEOUT);
    	
        // Need to ensure that this is set in the hadoop configuration so we can
        // instantiate a local client. The reason is that no files are in the
        // distributed cache when this is called.
        //
        //System.setProperty(ES_CONFIG, conf.get(ES_CONFIG));
        //System.setProperty(ES_PLUGINS, conf.get(ES_PLUGINS));

        LOG.info("ElasticSearchInputFormat: Starting embedded elasticsearch client ...");
		Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", clusterName)
            .put("client.transport.sniff", true)
            .build();
		TransportClient tcpClient = new TransportClient(settings);
		for (String host: hosts) {
			tcpClient.addTransportAddress(new InetSocketTransportAddress(host, this.port));
		}
		int i = 0;
		ImmutableList<DiscoveryNode> nodes = tcpClient.connectedNodes();
		for (DiscoveryNode node: nodes) {
			LOG.info("Connected to node " + (++i) + ": " + node.toString());
		}
        if (nodes.size() == 0) {
        	LOG.error("No nodes found");
        }
        this.client = tcpClient;

        SearchResponse response =
                client.prepareSearch(indexName).setTypes(objType).setSearchType(SearchType.COUNT)
                    .setQuery(QueryBuilders.wrapperQuery(queryString))
                    .execute().actionGet();
            this.numHits = response.hits().totalHits();
            if (numSplits > numHits) {
                numSplits = numHits; // This could be bad
            }
            this.numSplitRecords = (numHits / numSplits);
            
            LOG.info("ElasticSearchInputFormat: Request has " + numHits + " hits total, " 
            		+ numSplits + " splits, and " + numSplitRecords + " hits per split");
    }

    public Configuration getConf() {
        return conf;
    }

    protected class ElasticSearchRecordReader implements RecordReader<Text, Text> {
    	private Integer recordsRead;
        private Iterator<SearchHit> hitsItr;
    	private Long from;
        private Long recsToRead;

        private ElasticSearchRecordReader(InputSplit split) {
            queryString = ((ElasticSearchSplit) split).getQueryString();
            from        = ((ElasticSearchSplit) split).getFrom();
            recsToRead  = ((ElasticSearchSplit) split).getSize();
            recordsRead = 0;
            hitsItr = null;

            LOG.info("ElasticSearchRecordReader: Initialized elasticsearch record reader "
            		+ "on index [" + indexName
                    + "] and object type [" + objType + "] query [" + queryString
                    + "], from [" + from + "], size [" + recsToRead + "]");
        }

        private Iterator<SearchHit> fetchNextHits() {
            LOG.info("ElasticSearchRecordReader: fetchNextHits : index [" + indexName
                    + "] and object type [" + objType + "] query [" + queryString
                    + "], from [" + from + "], size [" + recsToRead + "]");
            SearchResponse response =
                client.prepareSearch(indexName).setTypes(objType).setFrom(from.intValue())
                    .setSize(recsToRead.intValue()).setTimeout(connect_timeout)
                    .setQuery(QueryBuilders.wrapperQuery(queryString))
                    .execute().actionGet();
            return response.hits().iterator();
        }

        public boolean next(Text key, Text val) throws IOException {
            if (hitsItr != null) {
                if (recordsRead < recsToRead) {
                    if (hitsItr.hasNext()) {
                        SearchHit hit = hitsItr.next();
                        key.set(hit.id());
                        val.set(hit.sourceAsString());
                        recordsRead += 1;
                        return true;
                    }
                } else {
                    hitsItr = null;
                }
            } else {
                if (recordsRead < recsToRead) {
                    hitsItr = fetchNextHits();
                    if (hitsItr.hasNext()) {
                        SearchHit hit = hitsItr.next();
                        key.set(hit.id());
                        val.set(hit.sourceAsString());
                        recordsRead += 1;
                        return true;
                    }
                }
            }
            return false;
        }

        public Text createKey() {
            return new Text();
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return 0;
        }

        public float getProgress() throws IOException {
            return recordsRead;
        }

        public void close() throws IOException {
            LOG.info("Closing record reader");
            client.close();
            LOG.info("Record reader closed.");
        }
    }
}