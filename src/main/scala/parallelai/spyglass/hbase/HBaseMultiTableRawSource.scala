package parallelai.spyglass.hbase

import cascading.pipe.Pipe
import cascading.pipe.assembly.Coerce
import cascading.scheme.Scheme
import cascading.tap.{ Tap, SinkMode }
import cascading.tuple.Fields
import org.apache.hadoop.mapred.{ RecordReader, OutputCollector, JobConf }
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray
import com.twitter.scalding._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.util.Base64
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

/**
 * @author Rotem Hermon
 *
 * HBaseMultiTableRawSource is a scalding source that passes the original row (Result) object to the
 * mapper for customized processing.
 *
 * @param	quorumNames	HBase quorum
 * @param	base64Scans	A list of scan objects in base64 string format
 * @param	familyNames	Column families to get (source, if null will get all) or update to (sink)
 * @param	writeNulls	Should the sink write null values. default = true. If false, null columns will not be written
 *
 */
class HBaseMultiTableRawSource(
	quorumNames: String = "localhost",
	base64Scans: Array[String],
	familyNames: Array[String] = null,
	writeNulls: Boolean = true) extends Source {

	override val hdfsScheme = new HBaseMultiTableRawScheme(familyNames, writeNulls)
		.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

	override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
		val hBaseScheme = hdfsScheme match {
			case hbase: HBaseMultiTableRawScheme => hbase
			case _ => throw new ClassCastException("Failed casting from Scheme to HBaseRawScheme")
		}
		mode match { 
			case hdfsMode @ Hdfs(_, _) => readOrWrite match {
				case Read => {
					new HBaseMultiTableRawTap(quorumNames, base64Scans, hBaseScheme).asInstanceOf[Tap[_, _, _]]
				}
				case Write => {
					new HBaseMultiTableRawTap(quorumNames, base64Scans, hBaseScheme).asInstanceOf[Tap[_, _, _]]
				}
			}
			case _ => super.createTap(readOrWrite)(mode)
		}
	}
}
