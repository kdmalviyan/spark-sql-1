package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.Date
import org.apache.spark.streaming.Minutes

/*  When an output operator is called, it triggers the computation of a stream.
   	print()
    foreachRDD(func)
    saveAsObjectFiles(prefix, [suffix])
    saveAsTextFiles(prefix, [suffix])
    saveAsHadoopFiles(prefix, [suffix])
*/
object StreamDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Streaming: Word count for incomming data every 5 seconds.")
      .setMaster("local[2]")
      .setSparkHome("/home/impadmin/hadoop/Spark/spark-2.1.0-bin-hadoop2.7")
      .set("spark.executor.memory", "2g");

    val streamContext = new StreamingContext(conf, Seconds(5))
    streamContext.checkpoint("checkpoint")
    val lines = streamContext.textFileStream("hdfs://localhost/usr/spark/stream/")
    lines.flatMap(line => line.split(" ")).map( word => (word, 1L)).reduceByKey(_ + _).print()
    //val linesfromLocal = streamContext.textFileStream("/home/impadmin/hadoop/Spark/datasets/stream/")
    
    streamContext.start()
    streamContext.awaitTermination()
  }
}