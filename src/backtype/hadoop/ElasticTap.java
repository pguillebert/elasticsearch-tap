package backtype.hadoop;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;

import java.io.IOException;


public class ElasticTap 
	extends Tap<JobConf, RecordReader, RecordWriter> {


    static Log LOG = LogFactory.getLog(ElasticTap.class);
	private static final long serialVersionUID = 9137969871650205244L;

	public ElasticTap(ElasticScheme scheme) {
		super(scheme);
	}
	
	public ElasticTap() {
		super(new ElasticScheme());
	}
	
    @Override
	public String getIdentifier() {
    	return "TBD";
	}

	@Override
	public long getModifiedTime(JobConf arg0) throws IOException {
		return 0;
	}

	@Override
	public boolean createResource(JobConf arg0) throws IOException {
		return true;
	}

	@Override
	public boolean deleteResource(JobConf arg0) throws IOException {
		return true;
	}

	@Override
	public boolean resourceExists(JobConf arg0) throws IOException {
		return true;
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> fp,
			RecordReader reader) throws IOException {
		LOG.info("ElasticTap: openForRead: reader " + reader.getClass().toString());
		return new HadoopTupleEntrySchemeIterator(fp, this, reader); 
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> fp,
			RecordWriter writer) throws IOException {
		LOG.info("ElasticTap: openForWrite");
		return new HadoopTupleEntrySchemeCollector(fp, (Tap) this);
	}
}
