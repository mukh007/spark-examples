package org.mj.scala.spark.streaming

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileExample {

  def main(args: Array[String]): Unit = {
    var dir = "/tmp/log/";
    if(args.length > 1) {
    	System.err.println("Usage: <log-dir>")
      dir = args(0)
    }

    val sparkConf = new SparkConf().setAppName("SpoolDirSpark").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val inputDirectory = dir

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory).map{ case (x, y) => (x.toString, y.toString) }

    lines.print()

    ssc.start()
    ssc.awaitTermination()

  }
}