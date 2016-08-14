package gg.masters


import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream, InputDStream}
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


  type SummonerChampionRoleToWinrate = ((Long, Int, String), (Float))

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  //Convert an rdd of type SummonerGame to an rdd of (summonerId, championId, Role) => (winRate)
  def summonerChampionRoleToWinrate(rdd: InputDStream[SummonerGame]): DStream[SummonerChampionRoleToWinrate] = {
    //Convert to a map of (summonerId, champion, role) => (wins, totalGames)
    //So we can calculate wins/totalGame later
    val intermediateMap = rdd.map(game => {
      game.winner match {
        case false => ((game.summonerId, game.championId, game.role), (0, 1))
        case true => ((game.summonerId, game.championId, game.role), (1, 1))
      }
    })

    //val reduced = intermediateMap.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))
    //This should be it:
    val reduced = intermediateMap.reduceByKeyAndWindow((x, y) => ((x._1 + y._1), (x._2 + y._2)), Seconds(20))

    //Finally we get the ratio map
    val summonerChampionRoleToWinRatioMap = reduced.mapValues(x => x._1.toFloat/x._2)

    //Sort by winRate
    //val sorted = summonerChampionRoleToWinRatioMap.map(_.swap).sortByKey(false).map(_.swap)
    val sorted = summonerChampionRoleToWinRatioMap.map(_.swap).transform(_.sortByKey(false)).map(_.swap)

    return sorted

  }

  def getKafkaSparkContext(ssc: StreamingContext):InputDStream[(String, String)] = {


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

    return messages
  }


  def main(args: Array[String]) {

    testWinrate()

//    val ssc = new StreamingContext("local[*]", "MostBoostedChampionAtRole", Seconds(5))
//
//    val messages = getKafkaSparkContext(ssc)
//
//    //ssc.checkpoint("/tmp")
//    ssc.start()
//    ssc.awaitTermination()

  }

  def testWinrate(): Unit ={
    val summonerGames = List[SummonerGame] (
      SummonerGame(1, 1, 1, "TOP", true),
      SummonerGame(1, 2, 2, "MIDDLE", true),
      SummonerGame(1, 3, 3, "JUNGLE", true),
      SummonerGame(1, 4, 4, "BOTTOM", true),
      SummonerGame(1, 5, 5, "SUPPORT", true),
      SummonerGame(1, 6, 6, "TOP", false),

      SummonerGame(2, 1, 1, "TOP", false),
      SummonerGame(2, 2, 2, "MIDDLE", true),

      SummonerGame(3, 1, 1, "TOP", false)
    )

    val ssc = new StreamingContext("local[*]", "MostBoostedChampionAtRole", Seconds(1))
    setupLogging()
    val rdd = ssc.sparkContext.parallelize(summonerGames)
    val stream = new ConstantInputDStream(ssc, rdd)
    summonerChampionRoleToWinrate(stream).print()

    ssc.start()
    ssc.awaitTermination()
  }

  // The code below should've worked if there was a "combieneByKeyAndWindow" method in spark streaming
  // I've found no way to do this but i'm keeping the code for now
//  def combiner(): Unit = {
//    //Now we have to reduce it to get the winrate for each (sum, champ, role)
//
//    val winRateInitial = (winner:Boolean) => {
//      winner match {
//        case false => (0, 1)
//        case true => (1, 1)
//      }
//
//    }
//
//    val winRateCombiner = (collector: (Int, Int), winner: Boolean) => {
//      winner match {
//        case false => (collector._1, collector._2 +1)
//        case true => (collector._1 + 1, collector._2 +1)
//      }
//    }
//
//    val winRateMerger = (collector1: (Int, Int), collector2: (Int, Int)) => {
//      (collector1._1 + collector2._1, collector2._1 + collector2._2)
//    }
//
//
//    val combined = summonerChampionRoleToWinnerMap.window(Seconds(10)).combineByKey(
//      winRateInitial,
//      winRateCombiner,
//      winRateMerger,
//      new HashPartitioner(1)
//    )
//
//    type aType = ((Long, Int, String), (Int, Int))
//
//    val rateFunction = (pair: ((Long, Int, String), (Int, Int))) => {
//      val ((summonerId, championId, role), (wins, total)) = pair
//      ((summonerId, championId, role), (wins /total))
//    }
//
//    val winrate = combined.print()
//  }
}
