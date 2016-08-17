package gg.boosted


import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}


object MostBoostedSummonersPlayingChampionAtRole  {


  type SummonerChampionRoleToWinrate = ((Long, Int, Role), (Float))

  type ChroleToSummonerWinrate = ((Int, Role), (Long, Float))

  val MIN_GAMES_WITH_CHROLE = 1


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

    run()
  }

  def run(): Unit = {
      val ssc = new StreamingContext("local[*]", "MostBoostedSummonersPlayingChampionAtRole", Seconds(1))

      val messages = Utilities.getKafkaSparkContext(ssc)

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

}
