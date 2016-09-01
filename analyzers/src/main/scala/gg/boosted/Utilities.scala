package gg.boosted

import com.robrua.orianna.`type`.core.common.Region
import com.robrua.orianna.api.core.RiotAPI
import gg.boosted.dal.RedisStore
import gg.boosted.posos.{SummonerChrole, SummonerMatch}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.msgpack.core.MessagePack
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by ilan on 8/16/16.
  */
object Utilities {

  val log = LoggerFactory.getLogger(Utilities.getClass)

  val RIOT_API_KEY = "840016c5-d254-4048-a608-d2b28b10e816"

  def unpackMessage(message: Array[Byte]):String = {
    MessagePack.newDefaultUnpacker(message).unpackString()
  }

  def getKafkaSparkContext(ssc: StreamingContext):DStream[(String, String)] = {

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "10.0.0.3:9092",
      "group.id" -> "group1",
      "auto.commit.interval.ms" -> "1000")

    val topics = "boostedgg"

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val deseredMessages = messages.mapValues(unpackMessage)

    return deseredMessages
  }

  def smRDDToDF(rdd:RDD[SummonerMatch]):DataFrame = {
    import Spark.session.implicits._
    rdd.toDF()
  }

  def rowToSummonerChrole(row: Row):SummonerChrole = {
    val summonerId = row.getLong(2)
    val summonerName = RedisStore.getSummonerNameById(summonerId).getOrElse({
      //Did not find the summonerName in the map, get it from riot
      RiotAPI.setAPIKey(RIOT_API_KEY)
      RiotAPI.setRegion(Region.valueOf(row.getString(3)))
      val summonerName = RiotAPI.getSummonerName(summonerId)
      log.debug(s"Retrieved from riot, summoner: $summonerId -> $summonerName")
      RedisStore.addSummonerName(summonerId, summonerName)
      summonerName
    })

    SummonerChrole(row.getInt(0), row.getInt(1), row.getLong(2), summonerName, row.getString(3), row.getInt(4), row.getLong(5), row.getDouble(6), row.getSeq[Long](7))
  }

}
