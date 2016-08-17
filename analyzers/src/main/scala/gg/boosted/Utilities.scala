package gg.boosted

import io.netty.handler.codec.bytes.ByteArrayDecoder
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.msgpack.core.MessagePack

/**
  * Created by ilan on 8/16/16.
  */
object Utilities {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def unpackMessage(message: Array[Byte]):String = {
    val unpacker =  MessagePack.newDefaultUnpacker(message);
    unpacker.unpackString()
  }

  def getKafkaSparkContext(ssc: StreamingContext):DStream[(String, String)] = {

    setupLogging()

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "10.0.0.3:9092",
      "group.id" -> "group1",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> "30000",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    val topics = "mastersgg"

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val deseredMessages = messages.mapValues(unpackMessage)

    return deseredMessages
  }

}
