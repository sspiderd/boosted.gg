package gg.boosted.utils

import gg.boosted.configuration.Configuration
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.msgpack.core.MessagePack
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/16/16.
  */
object KafkaUtil {

  val log = LoggerFactory.getLogger(KafkaUtil.getClass)


  def unpackMessage(message: Array[Byte]):String = {
    MessagePack.newDefaultUnpacker(message).unpackString()
  }

  def getKafkaSparkContext(ssc: StreamingContext):DStream[(String, String)] = {

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> Configuration.getString("kafka.location").concat(":9092"),
      "group.id" -> "group1",
      "auto.commit.interval.ms" -> "1000")

    val topics = Configuration.getString("kafka.topic")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val deseredMessages = messages.mapValues(unpackMessage)

    return deseredMessages
  }

}
