package com.spark.streaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaSparkWordCountDriver {
  def main(args: Array[String]) {
    val topics = "word_count"
    val zkQuorum = "localhost:2181"
    val group = "wordCount"
    val numThreads = 2
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.foreachRDD( words => words.foreach(word => println(word)))
    ssc.start()
    ssc.awaitTermination()
  }
}