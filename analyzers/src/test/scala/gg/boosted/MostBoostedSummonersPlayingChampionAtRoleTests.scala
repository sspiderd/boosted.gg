package gg.boosted

import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 8/15/16.
  */
class MostBoostedSummonersPlayingChampionAtRoleTests extends FlatSpec with BeforeAndAfter {

    private val master = "local[*]"
    private val appName = "MostBoostedSummonersPlayingChampionAtRoleTests"

    private var sc: SparkContext = _

    before {
        val conf = new SparkConf()
                .setMaster(master)
                .setAppName(appName)

        sc = new SparkContext(conf)
    }

    after {
        if (sc != null) {
            sc.stop()
        }
    }

    "A list with one game" should "return just 1 result" in {
        val rdd = sc.parallelize(Seq[SummonerGame] (SummonerGame(1,1,1,Role.TOP, true)))

        val result = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 0).collect()

        assert(result.size === 1)
    }
//
//    "This test" should "return summoner 2 is best at champion 1 at 'SUPPORT'" in {
//        val summonerGames = Seq[SummonerGame] (
//            SummonerGame(1, 1, 1, "TOP", true),
//            SummonerGame(1, 2, 1, "SUPPORT", true),
//            SummonerGame(2, 2, 1, "SUPPORT", true),
//            SummonerGame(3, 2, 1, "SUPPORT", true),
//            SummonerGame(4, 1, 1, "SUPPORT", true),
//            SummonerGame(5, 1, 1, "SUPPORT", false),
//            SummonerGame(6, 3, 1, "SUPPORT", false),
//            SummonerGame(6, 2, 1, "TOP", true)
//        )
//
//        val rdd = sc.parallelize(summonerGames) ;
//        val summonerChroleToWinrate = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 1)
//        val champion1ToSupportWR = MostBoostedSummonersPlayingChampionAtRole.championRoleToHighestWinrateSummoner(summonerChroleToWinrate, 1, "SUPPORT").collect()
//
//        //3 guys played champion 1 at role SUPPORT
//        assert(champion1ToSupportWR.size === 3)
//
//    }


}
