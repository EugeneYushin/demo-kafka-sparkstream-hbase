package demo

import java.io.File

import com.cloudera.spark.hbase.HBaseContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
// in order to pick up implicits that allow DStream.reduceByKey, ... (versus DStream.transform(rddBatch => rddBatch.reduceByKey())
// See https://www.mail-archive.com/user@spark.apache.org/msg10105.html
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object DemoDriver {

  def main(args: Array[String]) {
    val zkQuorum = "localhost:2181"
    val consumerGroup = "demo-consumer-group"
    val topic = "demo-stream-topic"
    val numPartitions = 1
    val numCores = numPartitions + 1  // one Reader per input DStream

    val sparkConf = new SparkConf()
      .setAppName("demo-kafka-sparkstream-hbase")
      .setMaster(s"local[$numCores]")
      .set("spark.cleaner.ttl", "120000")   // for long running tasks

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint(getCheckpointDir.toString)   // to fetch State for stateful RDD

    /*
    // Another way to create Reader for Kafka with explicit set offsets
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> consumerGroup,
      "zookeeper.connection.timeout.ms" -> "10000",
      "kafka.auto.offset.reset" -> "smallest"
    )

    val kafkaStream = KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, Map(topic -> numPartitions), storageLevel = StorageLevel.MEMORY_ONLY_SER)//.map(_._2)
    */

    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, consumerGroup, Map(topic -> numPartitions)).map(_._2.toString)
    val parsedStream = kafkaStream
//      .map[(String, (Long, Long, Long, Long))]({ event =>
//      val ts = event.split(",")(0)
//      val value = event.split(",")(1).toLong
//      (ts, (value, value, value, 1L))
//    })
      .map(_.split(","))
      .map{case x => (x(0).substring(0, x(0).indexOf(":",x(0).indexOf(":") + 1)), (x(1).toLong, x(1).toLong, x(1).toLong, 1L))}
      .reduceByKey((x,y) => (math.min(x._1, y._1), math.max(x._2, y._2), x._3 + y._3, x._4 + y._4))
      .updateStateByKey(calcAvgState)

    parsedStream.print()

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val hbaseContext = new HBaseContext(sc, conf)
    val hTableName = "demo-log"
    val hFamilyName = "demo-ts-metrics"

    hbaseContext.streamBulkPut[(String, (Long, Long, Long, Long, Double))](
      parsedStream, //The input RDD
      hTableName,   //The name of the table we want to put to
      (t) => {
        val put = new Put(Bytes.toBytes(t._1))
        put.add(Bytes.toBytes(hFamilyName), Bytes.toBytes("MIN"), Bytes.toBytes(t._2._1.toString))
        put.add(Bytes.toBytes(hFamilyName), Bytes.toBytes("MAX"), Bytes.toBytes(t._2._2.toString))
        put.add(Bytes.toBytes(hFamilyName), Bytes.toBytes("SUM"), Bytes.toBytes(t._2._3.toString))
        put.add(Bytes.toBytes(hFamilyName), Bytes.toBytes("CNT"), Bytes.toBytes(t._2._4.toString))
        put.add(Bytes.toBytes(hFamilyName), Bytes.toBytes("AVG"), Bytes.toBytes(t._2._5.toString))
        put
      },
      false)

    ssc.start()
    ssc.awaitTermination()

    FileUtils.deleteQuietly(getCheckpointDir)   // cleanup checkpoint dir
  }

  def calcAvgState(
    // (min, max, sum, counts)
    newValues: Seq[(Long, Long, Long, Long)],
    // (min, max, sum, counts, avg)
    prevValues: Option[(Long, Long, Long, Long, Double)]): Option[(Long, Long, Long, Long, Double)] = {

    var result: Option[(Long, Long, Long, Long, Double)] = null

    if (newValues.isEmpty) {
      result = prevValues
      return result
    }

    newValues.foreach(cur => {
      if (prevValues.isEmpty) {
        result = Some((cur._1, cur._2, cur._3, cur._4, 1.0 * cur._3 / cur._4))
      } else {
        result = Some((
            math.min(cur._1, prevValues.get._1)
          , math.max(cur._2, prevValues.get._2)
          , cur._3 + prevValues.get._3
          , cur._4 + prevValues.get._4
          , 1.0 * (cur._3 + prevValues.get._3) / (cur._4 + prevValues.get._4)
          ))
      }
    }
    )
    result
  }

  def getCheckpointDir : File = {
    val r = (new scala.util.Random).nextInt()
    val path = System.getProperty("java.io.tmpdir").toString //Seq(System.getProperty("java.io.tmpdir"), "spark-test-checkpoint-" + r).mkString(File.separator)
    new File(path)
  }

}
