package gg.boosted.utils;

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
      "group.id" -> "group3",
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
