package org.mj.scala.spark.streaming

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
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.KafkaRDD
import org.apache.spark.streaming.kafka.KafkaRDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkKafkaStreamingExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN);
  	val className = this.getClass().getSimpleName()
    var masterUrl = "local[2]"
    var brokerList = ""
    var zkQuorum = ""
    if (args.length > 2) {
      System.err.println("Usage: %s <broker-list> <zk-list> <topic>".format(className))
    } else {
      System.err.println("Usage: %s <%s> <%s> <%s>".format(className, masterUrl, brokerList, zkQuorum))
    }

    val Array(broker, zk, topic) = Array("localhost:9092", "localhost:2181/kafka", "good-messages")

    val sparkConf = new SparkConf().setAppName(className).setMaster(masterUrl)
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("file:///tmp/spark-checkpoint") // checkpointing dir

    val kafkaConf = Map("metadata.broker.list" -> broker,
                        "zookeeper.connect" -> zk,
                        "auto.offset.reset" -> "smallest",
                        "zookeeper.connection.timeout.ms" -> "1000")

    val directKafkaStream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set(topic)).map(_._2)

//    var offsetRanges = Array[OffsetRange]()
//    directKafkaStream.transform( rdd =>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    ).foreachRDD( rdd =>
//      for (o <- offsetRanges) {
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      }
//    )

    val words = directKafkaStream.flatMap(_.split(" "))

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
      rdd.foreach({
                    case (value, count) => {
                      println("##########################################")
                      println("value --> " + value + " with count --> " + count)
                      println("##########################################")
                    }
                  })
    })

    wordCounts.print()

    //  Getting Kafka offsets from RDDs
//    var offsetRanges = Array[OffsetRange]()
    
    directKafkaStream.foreachRDD { rdd =>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("MJ1 " + rdd)
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("MJ ")
//      for (o <- offsetRanges) {
//      	println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      }
    }

    
    ssc.start()
    ssc.awaitTermination()
  }
}
