package gg.masters


import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s._
import org.json4s.jackson.Json
import org.json4s.jackson.JsonMethods._


/**
 * Hello world!
 *
 */
object MostBoostedChampionAtRole  {


  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "MostBoostedChampionAtRole", Seconds(10))

    setupLogging()

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "10.0.0.3:9092",
      "group.id" -> "group1",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> "30000",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    val topics = "mastersgg"

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //Convert to a list of summonerGame objects
    val summonerGames = messages.map(message => Converter.toSummonerGame(message._2))

    //Convert to a map of (champion, role) => (summonerId, winner)

    //Test to see how many games were played at that role //
    val championAtRoleMap = summonerGames.map(game => ((game.championId, game.role), 1))

    championAtRoleMap.reduceByKeyAndWindow(((x,y) => x + y), Seconds(200)) ;

    championAtRoleMap.print()


    ssc.start()
    ssc.awaitTermination()


  }
}
