package gg.boosted

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 8/15/16.
  */
class MostBoostedSummonersPlayingChampionAtRoleTests extends FlatSpec with BeforeAndAfter {

    private val master = "local[*]"
    private val appName = "MostBoostedSummonersPlayingChampionAtRoleTests"

    private val now = new Date().getTime ;

    private var sc: SparkContext = _

    before {
        val spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate()

        sc = spark.sparkContext
    }

    after {
        if (sc != null) {
            sc.stop()
        }
    }

    "A list with one game" should "return just 1 result" in {
        val rdd = sc.parallelize(Seq[SummonerMatch] (SummonerMatch(1,1,1,Role.TOP, true, "NA", now, Tier.GOLD)))

        val result = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 0, 0).collect()

        assert(result.length === 1)
    }

    "Second parameter to summonerChampionRoleToWinRate" should "filter out losers below threshold" in {
        val rdd = sc.parallelize(Seq[SummonerMatch] (SummonerMatch(1,1,1,Role.TOP, true, "NA", now, Tier.GOLD)))

        val result = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 1, 0).collect()

        assert(result.length === 1)

        val rdd2 = sc.parallelize(Seq[SummonerMatch] (
            SummonerMatch(1,1,1,Role.TOP, true, "NA", now, Tier.GOLD),
            SummonerMatch(1,1,1,Role.TOP, false, "NA", now, Tier.GOLD)
        ))

        val result2 = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd2, 1, 0).collect()

        assert(result2.length === 1)
    }

    "Win rate for summoners 1, 2, 3" should "be 1/0.5/0 respectively" in {
      val rdd = sc.parallelize(Seq[SummonerMatch] (
        SummonerMatch(1,1,1,Role.TOP, true, "NA", now, Tier.GOLD),
        SummonerMatch(1,2,2,Role.MIDDLE, true, "NA", now, Tier.GOLD),
        SummonerMatch(1,3,3,Role.JUNGLE, false, "NA", now, Tier.GOLD),
        SummonerMatch(2,1,1,Role.TOP, true, "NA", now, Tier.GOLD),
        SummonerMatch(2,2,2,Role.MIDDLE, false, "NA", now, Tier.GOLD),
        SummonerMatch(2,3,3,Role.JUNGLE, false, "NA", now, Tier.GOLD)
      ))

      val result = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 1, 0).collect()

      assert(result.length === 3)

      def getSummonerById(id: Long, arr: Array[((Long, Int, Role), (Float))]):((Long, Int, Role), (Float)) = {
        arr.filter(_._1._1 == id)(0)
      }

      assert(getSummonerById(1, result)._2 === 1)
      assert(getSummonerById(2, result)._2 === 0.5)
      assert(getSummonerById(3, result)._2 === 0)
    }


    "Summoners 2, 1, 3" should "be 1st, 2nd and 3rd as top supports respectively" in {
      val summonerGames = Seq[SummonerMatch] (
          SummonerMatch(1, 1, 1, Role.TOP, true, "NA", now, Tier.GOLD),
          SummonerMatch(1, 2, 1, Role.SUPPORT, true, "NA", now, Tier.GOLD),
          SummonerMatch(2, 2, 1, Role.SUPPORT, true, "NA", now, Tier.GOLD),
          SummonerMatch(3, 2, 1, Role.SUPPORT, true, "NA", now, Tier.GOLD),
          SummonerMatch(4, 1, 1, Role.SUPPORT, true, "NA", now, Tier.GOLD),
          SummonerMatch(5, 1, 1, Role.SUPPORT, false, "NA", now, Tier.GOLD),
          SummonerMatch(6, 3, 1, Role.SUPPORT, false, "NA", now, Tier.GOLD),
          SummonerMatch(6, 2, 1, Role.TOP, true, "NA", now, Tier.GOLD)
      )

      val rdd = sc.parallelize(summonerGames) ;
      val summonerChroleToWinrate = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 1, 0)
      val champion1ToSupportWR = MostBoostedSummonersPlayingChampionAtRole.championRoleToHighestWinrateSummoner(summonerChroleToWinrate, 1, Role.SUPPORT).collect()

      //3 guys played champion 1 at role SUPPORT
      assert(champion1ToSupportWR.size === 3)
      assert(champion1ToSupportWR(0)._1 === 2)
      assert(champion1ToSupportWR(1)._1 === 1)
      assert(champion1ToSupportWR(2)._1 === 3)

    }

    "Games not in timespan" should "be filtered out" in {
      val summonerGames = Seq[SummonerMatch] (
        SummonerMatch(1, 1, 1, Role.TOP, true, "NA", now, Tier.GOLD),
        SummonerMatch(2, 1, 1, Role.TOP, false, "NA", 0, Tier.GOLD),
        SummonerMatch(2, 2, 1, Role.SUPPORT, false, "NA", 0, Tier.GOLD)
      )

      val rdd = sc.parallelize(summonerGames)

      val summonerChroleToWR = MostBoostedSummonersPlayingChampionAtRole.summonerChampionRoleToWinrate(rdd, 1, now - 10000).collect()

      assert(summonerChroleToWR.length === 1)
      assert(summonerChroleToWR(0)._2 === 1)

    }


}
