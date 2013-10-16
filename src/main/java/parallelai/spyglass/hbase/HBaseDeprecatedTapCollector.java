/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parallelai.spyglass.hbase;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class HBaseDeprecatedTapCollector is a kind of
 * {@link cascading.tuple.TupleEntrySchemeCollector} that writes tuples to the
 * resource managed by a particular {@link HBaseTap} instance. It is intended
 * for use when using DeprecatedOutputFormatWrapper for old mapreduce
 * OutputFormat.
 */
public class HBaseDeprecatedTapCollector extends TupleEntrySchemeCollector implements OutputCollector {
	/** Field LOG */
	private static final Logger LOG = LoggerFactory.getLogger(HBaseDeprecatedTapCollector.class);
	/** Field conf */
	private final JobConf conf;
	/** Field writer */
	private RecordWriter writer;
	/** Field flowProcess */
	private final FlowProcess<JobConf> hadoopFlowProcess;
	/** Field tap */
	private final Tap<JobConf, RecordReader, OutputCollector> tap;
	private final FlowStatusReporter reporter = new FlowStatusReporter();
	
	class FlowStatusReporter extends StatusReporter implements Progressable {

		FlowStatusReporter() {
		}

		@Override
		public Counter getCounter(Enum<?> name) {
			if (hadoopFlowProcess instanceof HadoopFlowProcess)
			{
				return ((HadoopFlowProcess) hadoopFlowProcess).getReporter().getCounter(name);
			}
			return null;
		}

		@Override
		public Counter getCounter(String group, String name) {
			if (hadoopFlowProcess instanceof HadoopFlowProcess)
			{
				return ((HadoopFlowProcess) hadoopFlowProcess).getReporter().getCounter(group, name);
			}
			return null;
		}

		@Override
		public void progress() {
			if (hadoopFlowProcess instanceof HadoopFlowProcess)
			{
				((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();
			}
		}

		@Override
		public void setStatus(String status) {
			hadoopFlowProcess.setStatus(status);
		}

		public Reporter getReporter()
		{
			if (hadoopFlowProcess instanceof HadoopFlowProcess)
			{
				return ((HadoopFlowProcess) hadoopFlowProcess).getReporter();
			}
			return null;
		}
	}

	/**
	 * Constructor TapCollector creates a new TapCollector instance.
	 * 
	 * @param flowProcess
	 * @param tap
	 *            of type Tap
	 * @throws IOException
	 *             when fails to initialize
	 */
	public HBaseDeprecatedTapCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap)
			throws IOException {
		super(flowProcess, tap.getScheme());
		this.hadoopFlowProcess = flowProcess;
		this.tap = tap;
		this.conf = new JobConf(flowProcess.getConfigCopy());
		this.setOutput(this);
	}

	@Override
	public void prepare() {
		try {
			initialize();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		super.prepare();
	}

	private void initialize() throws IOException {
		tap.sinkConfInit(hadoopFlowProcess, conf);
		OutputFormat outputFormat = conf.getOutputFormat();
		LOG.info("Output format class is: " + outputFormat.getClass().toString());
		writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(), reporter);
		sinkCall.setOutput(this);
	}

	@Override
	public void close() {
		try {
			LOG.info("closing tap collector for: {}", tap);
			writer.close(reporter.getReporter());
		} catch (IOException exception) {
			LOG.warn("exception closing: {}", exception);
			throw new TapException("exception closing HBaseTapCollector", exception);
		} finally {
			super.close();
		}
	}

	/**
	 * Method collect writes the given values to the {@link Tap} this instance
	 * encapsulates.
	 * 
	 * @param writableComparable
	 *            of type WritableComparable
	 * @param writable
	 *            of type Writable
	 * @throws IOException
	 *             when
	 */
	public void collect(Object writableComparable, Object writable) throws IOException {
		if (hadoopFlowProcess instanceof HadoopFlowProcess)
			((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();

		writer.write(writableComparable, writable);
	}
}
