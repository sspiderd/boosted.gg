package gg.boosted


import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object MostBoostedSummonersPlayingChampionAtRole  {


  type SummonerChampionRoleToWinrate = ((Long, Int, Role), (Float))

  type ChroleToSummonerWinrate = ((Int, Role), (Long, Float))

  val MIN_GAMES_WITH_CHROLE = 8

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
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


  //Convert an rdd of type SummonerGame to an rdd of (summonerId, championId, Role) => (winRate)
  //def summonerChampionRoleToWinrate(rdd: DStream[SummonerGame]): DStream[SummonerChampionRoleToWinrate] = {
//  def summonerChampionRoleToWinrateStream(rdd: DStream[SummonerGame]): DStream[SummonerChampionRoleToWinrate] = {
//    //Convert to a map of (summonerId, champion, role) => (wins, totalGames)
//    //So we can calculate wins/totalGame later
//    val intermediateMap = rdd.map(game => {
//      game.winner match {
//        case false => ((game.summonerId, game.championId, game.role), (0, 1))
//        case true => ((game.summonerId, game.championId, game.role), (1, 1))
//      }
//    })
//
//    //val reduced = intermediateMap.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))
//    //This should be it:
//    val reduced = intermediateMap.reduceByKeyAndWindow((x, y) => ((x._1 + y._1), (x._2 + y._2)), Seconds(2))
//    //val reduced = intermediateMap.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))
//
//    //Finally we get the ratio map
//    val summonerChampionRoleToWinRatioMap = reduced.mapValues(x => x._1.toFloat/x._2)
//
//    //Sort by winRate
//    //val sorted = summonerChampionRoleToWinRatioMap.map(_.swap).sortByKey(false).map(_.swap)
//    val sorted = summonerChampionRoleToWinRatioMap.map(_.swap).transform(_.sortByKey(false)).map(_.swap)
//
//    return sorted
//
//  }

  //Convert an rdd of type SummonerGame to an rdd of (summonerId, championId, Role) => (winRate)
  //def summonerChampionRoleToWinrate(rdd: DStream[SummonerGame]): DStream[SummonerChampionRoleToWinrate] = {
  def summonerChampionRoleToWinrate(rdd: RDD[SummonerGame], minGamesWithChrole:Int): RDD[SummonerChampionRoleToWinrate] = {
    //Convert to a map of (summonerId, champion, role) => (wins, totalGames)
    //So we can calculate wins/totalGame later
    val intermediateMap = rdd.map(game => {
      game.winner match {
        case false => ((game.summonerId, game.championId, game.role), (0, 1))
        case true => ((game.summonerId, game.championId, game.role), (1, 1))
      }
    })

    //This will give us a map of summonerChrole -> (wins, total games)
    val reduced = intermediateMap.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))

    //We are interested only in summoners that played that chrole more than ${min_games_with_chrole} times
    val filtered = reduced.filter(_._2._2 >= minGamesWithChrole)

    //Finally we get the ratio map
    val summonerChampionRoleToWinRatioMap = filtered.mapValues(x => x._1.toFloat/x._2)

    return summonerChampionRoleToWinRatioMap

  }

  def championRoleToHighestWinrateSummoner(rdd: RDD[SummonerChampionRoleToWinrate], championId:Int, role: Role):RDD[(Long, Float)] = {

    //Get only the chrole we want from this list
    //._1._2 is the championId for this rdd and ._1._3 is the role
    val filteredChrole = rdd.filter(line => line._1._2 == championId && line._1._3 == role)

    val summonerIdToWinrateSorted = filteredChrole.map(line => (line._2, line._1._1)).sortByKey(false).map(_.swap)

    return summonerIdToWinrateSorted
  }

  def main(args: Array[String]) {

   //testWinrate()

    run()

    //runStream()
  }

  def run(): Unit = {
      val ssc = new StreamingContext("local[*]", "MostBoostedSummonersPlayingChampionAtRole", Seconds(1))

      val messages = getKafkaSparkContext(ssc)

      val result = messages.map(_._2).window(Seconds(10), Seconds(1))
              .map(SummonerGame(_))
              .transform ( rdd => summonerChampionRoleToWinrate(rdd, MIN_GAMES_WITH_CHROLE))

      result.foreachRDD(rdd => {
        rdd.foreach(println)
        println("-----")
      })


      //ssc.checkpoint("/tmp")
      ssc.start()
      ssc.awaitTermination()
  }

//  def runStream(): Unit = {
//    val ssc = new StreamingContext("local[*]", "MostBoostedChampionAtRole", Seconds(1))
//
//    val messages = getKafkaSparkContext(ssc)
//
//    val result = summonerChampionRoleToWinrateStream(messages.map(_._2).map(SummonerGame(_)))
//
//
//    result.foreachRDD(rdd => {
//      rdd.foreach(println)
//      println("-----")
//    })
//
//
//    //ssc.checkpoint("/tmp")
//    ssc.start()
//    ssc.awaitTermination()
//  }

//  def testWinrate(): Unit ={
//    val summonerGames = Seq[SummonerGame] (
//      SummonerGame(1, 1, 1, "TOP", false),
//      SummonerGame(1, 2, 2, "MIDDLE", false)
//    )
//
//    val summonerGame2 = Seq[SummonerGame] (
//      SummonerGame(2, 1, 1, "TOP", false),
//      SummonerGame(2, 2, 2, "MIDDLE", true)
//    )
//
//
//
//    val ssc = new StreamingContext("local[*]", "MostBoostedChampionAtRole", Seconds(1))
//    setupLogging()
//
//    val rdd1 = ssc.sparkContext.parallelize(summonerGames)
//    val rdd2 = ssc.sparkContext.parallelize(summonerGame2)
//
//    val q = scala.collection.mutable.Queue[RDD[SummonerGame]] (rdd1, rdd2)
//
//    val stream = ssc.queueStream(q, true) ;
//    //summonerChampionRoleToWinrate(stream).print()
//    summonerChampionRoleToWinrate(rdd2, MIN_GAMES_WITH_CHROLE).foreach(println(_))
//
//    //ssc.start()
//    //ssc.awaitTermination()
//  }

}
