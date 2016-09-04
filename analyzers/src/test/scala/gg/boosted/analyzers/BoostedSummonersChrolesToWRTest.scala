package gg.boosted.analyzers

import java.util.Date

import gg.boosted.posos.SummonerMatch
import gg.boosted.{Role, Tier}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}


/**
  * Created by ilan on 8/25/16.
  */
class BoostedSummonersChrolesToWRTest extends FlatSpec with BeforeAndAfter{

    private val master = "local[*]"
    private val appName = "BoostedSummonersChrolesToWRTest"

    private val now = new Date().getTime ;

    private val spark:SparkSession = SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .getOrCreate()

    import spark.implicits._

    "A list with one win" should "return just 1 result" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId))
        )

        val result = BoostedSummonersChrolesToWR.calc(df, 0, 0).collect()

        assert(result.length === 1)
    }

    "A list with one loss" should "return no results" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, false, "NA", now, Tier.GOLD.tierId))
        )

        val result = BoostedSummonersChrolesToWR.calc(df, 0, 0).collect()

        assert(result.length === 0)
    }

    "A list with a winner and a loser" should "filter out the loser" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(1,2,1,Role.TOP.roleId, false, "NA", now, Tier.GOLD.tierId)
        ))

        val result = BoostedSummonersChrolesToWR.calc(df, 0, 0).collect()
        assert(result.length === 1)
    }

    "A list with someone that hasn't played enough games" should "be filtered out" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId)
        ))

        val result = BoostedSummonersChrolesToWR.calc(df, 2, 0).collect()
        assert(result.length === 0)
    }

    "People who haven't played enough games" should "be fitlered out" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(3,2,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId)
        ))

        val result = BoostedSummonersChrolesToWR.calc(df, 2, 0).collect()
        assert(result.length === 1)
        //The one who is left is player number 2
        assert(result(0)(2) === 2)
    }

    "A list with an old game" should "be filtered out" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", 0, Tier.GOLD.tierId)
        ))

        val result = BoostedSummonersChrolesToWR.calc(df, 0, 1).collect()
        assert(result.length === 0)
    }


    "When there are many winners they" should "be ordered by winrate desc" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            //100%
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),

            //66%
            SummonerMatch(4,2,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(5,2,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(6,2,1,Role.TOP.roleId, false, "NA", now, Tier.GOLD.tierId),

            //75%
            SummonerMatch(7,3,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(8,3,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(9,3,1,Role.TOP.roleId, false, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(10,3,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),

            //50% -> Should be filtered out
            SummonerMatch(11,4,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(12,4,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(13,4,1,Role.TOP.roleId, false, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(14,4,1,Role.TOP.roleId, false, "NA", now, Tier.GOLD.tierId)
        ))

        val result = BoostedSummonersChrolesToWR.calc(df, 1, 0).as[BoostedSummonersChrolesToWR].collect()
        assert(result.length === 3)

        assert(result(0).summonerId === 1)
        assert(result(0).gamesPlayed === 1)
        assert(result(0).winrate === 1)

        assert(result(1).summonerId === 3)
        assert(result(1).gamesPlayed === 4)
        assert(result(1).winrate === 3.0/4)

        assert(result(2).summonerId === 2)
        assert(result(2).gamesPlayed === 3)
        assert(result(2).winrate === 2.0/3)
    }

    "Lists with multiple chroles" should "be filtered correctly" in {
        val df = spark.createDataFrame[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId),
            SummonerMatch(2,2,2,Role.TOP.roleId, true, "NA", now, Tier.GOLD.tierId)
        ))

        val calcResult = BoostedSummonersChrolesToWR.calc(df, 1, 0).cache()

        assert (calcResult.collect().length === 2)

        val filteredByChamp1 = BoostedSummonersChrolesToWR.filterByChrole(calcResult, 1, Role.TOP.roleId).as[BoostedSummonersChrolesToWR].collect()
        val filteredByChamp2 = BoostedSummonersChrolesToWR.filterByChrole(calcResult, 2, Role.TOP.roleId).as[BoostedSummonersChrolesToWR].collect()

        assert(filteredByChamp1.length === 1)
        assert(filteredByChamp1(0).championId === 1)

        assert(filteredByChamp2.length === 1)
        assert(filteredByChamp2(0).championId === 2)
    }

}
