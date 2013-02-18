package backtype.hadoop;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.beans.ConstructorProperties;
import java.io.IOException;

public class ElasticScheme extends Scheme<JobConf,RecordReader,
									RecordWriter,Object[],Object[]> {

    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** Field DEFAULT_FIELDS */
    public static final Fields DEFAULT_FIELDS = new Fields("id", "json");
	static Log LOG = LogFactory.getLog(ElasticScheme.class);

    public ElasticScheme() {
        super(DEFAULT_FIELDS);
    }

    /**
     * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all
     * incoming fields, where "offset" is the byte offset in the input file.
     *
     * @param numSinkParts of type int
     */
    @ConstructorProperties({"numSinkParts"})
    public ElasticScheme(int numSinkParts) {
        super(DEFAULT_FIELDS, numSinkParts);
    }

    @ConstructorProperties({"field"})
    public ElasticScheme(Fields field) {
        super(field, field);

        if (field.size() != 2) {
            throw new IllegalArgumentException(
                "this scheme requires 2 fields, not [" + field + "]");
        }
    }

    @ConstructorProperties({"field", "numSinkParts"})
    public ElasticScheme(Fields field, int numSinkParts) {
        super(field, numSinkParts);

        if (field.size() != 2) {
            throw new IllegalArgumentException(
                "this scheme requires 2 fields, not [" + getSourceFields() + "]");
        }
    }

	@Override
	public void sinkConfInit(
			FlowProcess<JobConf> arg0,
			Tap<JobConf, RecordReader, RecordWriter> arg1,
			JobConf jobConf) {
		jobConf.setOutputKeyClass(NullWritable.class); // be explicit
        jobConf.setOutputValueClass(MapWritable.class); // be explicit
        jobConf.setOutputFormat(ElasticSearchOutputFormat.class);
        LOG.info(String.format("Initializing ElasticSearch sink tap - field: %s", getSinkFields()));
	}

	@Override
	public void sourceConfInit(
			FlowProcess<JobConf> fp,
			Tap<JobConf, RecordReader, RecordWriter> arg1,
			JobConf jobConf) {

		jobConf.setInputFormat(ElasticSearchInputFormat.class);

		//  Map<Object, Object> properties = HadoopUtil.createProperties(jobConf);
		//  properties.remove( "mapred.input.dir" );
		//  jobConf = HadoopUtil.createJobConf( properties, null );
		//  fp.copyWith(jobConf);
        
        LOG.info(String.format("Initializing ElasticSearch source tap - field: %s", getSourceFields()));
        }

	@Override
	public void sink(FlowProcess<JobConf> arg0,
			SinkCall<Object[], RecordWriter> sinkCall)
			throws IOException {
		
		LOG.warn("ElasticScheme: Sink not implemented yet.");
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
        MapWritable record = new MapWritable();
        if (tupleEntry != null) {

/*        	
        	String jsonData = tupleEntry.getString(DEFAULT_FIELDS);

         	// parse json data and put into MapWritable record
           try {
                ObjectMapper mapper = new ObjectMapper();
                HashMap<String, Object> data = mapper.readValue(jsonData, HashMap.class);
                record = (MapWritable) ElasticUtil.toWritable(data);
            } catch (JsonParseException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            }
*/        }
        sinkCall.getOutput().write(NullWritable.get(), record);
	}
	
	@Override
	public boolean source(FlowProcess<JobConf> arg0,
			SourceCall<Object[], RecordReader> sourceCall)
			throws IOException {

		Text key = new Text();
		Text value = new Text();

        boolean result = sourceCall.getInput().next( key, value );

        if( !result )
            return false;

        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        tuple.clear();

        tuple.add( key.toString() );
        tuple.add( value.toString() );

        return true;
	}
}
