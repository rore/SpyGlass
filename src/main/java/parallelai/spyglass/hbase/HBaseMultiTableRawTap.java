/*
* Copyright (c) 2009 Concurrent, Inc.
*
* This work has been released into the public domain
* by the copyright holder. This applies worldwide.
*
* In case this is not legally possible:
* The copyright holder grants any entity the right
* to use this work for any purpose, without any
* conditions, unless such conditions are required by law.
*/

package parallelai.spyglass.hbase;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;


/**
* The HBaseRawTap class is a {@link Tap} subclass. It is used in conjunction with
* the {@HBaseMultiTableRawScheme} to allow for the reading and writing
* of data to and from a HBase cluster.
*/
@SuppressWarnings({ "deprecation", "rawtypes" })
public class HBaseMultiTableRawTap extends Tap<JobConf, RecordReader, OutputCollector> {
	/**
	 *
	 */
	private static final long serialVersionUID = 8019189493428493323L;

	/** Field LOG */
	private static final Logger LOG = LoggerFactory.getLogger(HBaseMultiTableRawTap.class);

	private final String id = UUID.randomUUID().toString();

	/** Field SCHEME */
	public static final String SCHEME = "hbase";

	/** Field hBaseAdmin */
	private transient HBaseAdmin hBaseAdmin;

	/** Field hostName */
	private String quorumNames;
	private String[] rawScans;

	/**
	 * Constructor HBaseTap creates a new HBaseTap instance.
	 *
	 * @param tableName
	 *            of type String
	 * @param HBaseFullScheme
	 *            of type HBaseFullScheme
	 */
	public HBaseMultiTableRawTap(String[] scans, HBaseMultiTableRawScheme HBaseFullScheme) {
		super(HBaseFullScheme, SinkMode.UPDATE);
		this.rawScans = scans;
	}

	/**
	 * Constructor HBaseTap creates a new HBaseTap instance.
	 *
	 * @param tableName
	 *            of type String
	 * @param HBaseFullScheme
	 *            of type HBaseFullScheme
	 * @param sinkMode
	 *            of type SinkMode
	 */
	public HBaseMultiTableRawTap(String[] scans, HBaseMultiTableRawScheme HBaseFullScheme, SinkMode sinkMode) {
		super(HBaseFullScheme, sinkMode);
		this.rawScans = scans;
	}

	/**
	 * Constructor HBaseTap creates a new HBaseTap instance.
	 *
	 * @param tableName
	 *            of type String
	 * @param HBaseFullScheme
	 *            of type HBaseFullScheme
	 */
	public HBaseMultiTableRawTap(String quorumNames, String[] scans, HBaseMultiTableRawScheme HBaseFullScheme) {
		super(HBaseFullScheme, SinkMode.UPDATE);
		this.quorumNames = quorumNames;
		this.rawScans = scans;
	}

	/**
	 * Constructor HBaseTap creates a new HBaseTap instance.
	 *
	 * @param tableName
	 *            of type String
	 * @param HBaseFullScheme
	 *            of type HBaseFullScheme
	 * @param sinkMode
	 *            of type SinkMode
	 */
	public HBaseMultiTableRawTap(String quorumNames, String[] scans, HBaseMultiTableRawScheme HBaseFullScheme, SinkMode sinkMode) {
		super(HBaseFullScheme, sinkMode);
		this.quorumNames = quorumNames;
		this.rawScans = scans;
	}


	public Path getPath() {
		String scanPath = "";
		for (String scan : rawScans)
		{
			scanPath += scan.replaceAll(":", "_");
		}
		return new Path(SCHEME + ":/" + scanPath);
	}

	protected HBaseAdmin getHBaseAdmin(JobConf conf) throws MasterNotRunningException, ZooKeeperConnectionException {
		if (hBaseAdmin == null) {
			Configuration hbaseConf = HBaseConfiguration.create(conf);
			hBaseAdmin = new HBaseAdmin(hbaseConf);
		}

		return hBaseAdmin;
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
		if (quorumNames != null) {
			conf.set("hbase.zookeeper.quorum", quorumNames);
		}
		super.sinkConfInit(process, conf);
	}

	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess, RecordReader recordReader)
			throws IOException {
		return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess, OutputCollector outputCollector)
			throws IOException {
		HBaseDeprecatedTapCollector hBaseCollector = new HBaseDeprecatedTapCollector(jobConfFlowProcess, this);
		hBaseCollector.prepare();
		return hBaseCollector;
	}

	@Override
	public long getModifiedTime(JobConf jobConf) throws IOException {
		return System.currentTimeMillis(); // currently unable to find last mod
											// time
											// on a table
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
		if (null == rawScans) throw new IllegalArgumentException("scans cannot be null");
		for (String scan : rawScans)
		{
			Path p = new Path(SCHEME + ":/" + scan.replaceAll(":", "_"));
			FileInputFormat.addInputPath(conf,  p);
		}

		if (quorumNames != null) {
			conf.set("hbase.zookeeper.quorum", quorumNames);
		}

		conf.setStrings(MultiTableInputFormat.SCANS, rawScans);

		super.sourceConfInit(process, conf);
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (object == null || getClass() != object.getClass()) {
			return false;
		}
		if (!super.equals(object)) {
			return false;
		}

		HBaseMultiTableRawTap hBaseTap = (HBaseMultiTableRawTap) object;

		if (rawScans != null ? !rawScans.equals(hBaseTap.rawScans) : hBaseTap.rawScans != null) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (rawScans != null ? rawScans.hashCode() : 0) + (rawScans != null ? rawScans.hashCode() : 0);
		return result;
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteResource(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean resourceExists(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}
}
