package org.mj.scala.spark

import java.util

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Writable, IntWritable, Text}
import org.apache.hadoop.mapred.{TextOutputFormat, JobConf}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object KafkaExample {

  def main(args: Array[String]): Unit = {
  	val className = this.getClass().getSimpleName()
    var masterUrl = "local[2]"
    var Array(broker, zk, topic) = Array("chsnmvproc71vm3.usdc2.oraclecloud.com:9092", "chsnmvproc71vm3.usdc2.oraclecloud.com:2181/kafka", "good-messages")

    System.err.println("Usage: %s <%s> <%s> <%s>".format(className, masterUrl, broker, zk, topic))

    val sparkConf = new SparkConf().setAppName(className).setMaster(masterUrl)
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaConf = Map("metadata.broker.list" -> broker,
                        "zookeeper.connect" -> zk,
                        "group.id" -> "kafka-spark-streaming-example",
                        "zookeeper.connection.timeout.ms" -> "1000")

    /* Kafka integration with reciever */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Map(topic -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    //  .reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(2), 2)
    //wordCounts.print()

    /*words.map(x => (x, 1L)).saveAsNewAPIHadoopFiles(
      "prefix", "suffix",
      classOf[Text],
      classOf[IntWritable],
      classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[Text]],
      conf)*/

    wordCounts.foreachRDD ( rdd => {

/* // HBASE 
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "stream_count")
      conf.set("hbase.zookeeper.quorum", "localhost:2181")
      conf.set("hbase.master", "localhost:60000");
      conf.set("hbase.rootdir", "file:///tmp/hbase")

      val jobConf = new Configuration(conf)
      jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
      jobConf.set("mapreduce.job.output.value.class", classOf[LongWritable].getName)
      jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)

      rdd.saveAsNewAPIHadoopDataset(jobConf)
*/
      //rdd.saveAsTextFile("/user/vagrant/tmp/sparktest_out")
      //new PairRDDFunctions(rdd.map(convert)).saveAsNewAPIHadoopDataset(jobConf)
///*
       rdd.foreach({
                    case (value, count) => {
                      println("##########################################")
                      println("value --> " + value + " with count --> " + count)
                      println("##########################################")
                    }
                  })
//*/
      //val connection = connect("stream_count")
      //rdd.foreach( record => connection.put(putRequest(record)) )
    })

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def putRequest(t: (String, Long)) = {
    val p = new Put(Bytes.toBytes(t._1))
    p.add(Bytes.toBytes("word"), Bytes.toBytes("count"), Bytes.toBytes(t._2))
  }

  def convert(t: (String, Long)) = {
    val p = new Put(Bytes.toBytes(t._1))
    p.add(Bytes.toBytes("word"), Bytes.toBytes("count"), Bytes.toBytes(t._2))
    (t._1, p)
  }
}
