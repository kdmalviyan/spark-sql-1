package com.spark.streaming.kafka

import java.util.HashMap

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaProducer {
  def main(args: Array[String]) {
    println("Starting kafka producer")
    val brokers = "localhost:9092,localhost:9093";
    val topic = "word_count"
    val messagesPerSec = 1
    val wordsPerMessage = 10;

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while (true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(5).toString + " Test kafka spark streams.")
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        println("message produced=> " + message)
        producer.send(message)
      }
      println("Sleeping for 5000 ms")
      Thread.sleep(5000)
    }
  }
}