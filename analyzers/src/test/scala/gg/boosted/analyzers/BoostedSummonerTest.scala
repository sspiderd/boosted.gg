package gg.boosted.analyzers

import java.util.Date

import gg.boosted.posos.SummonerMatch
import gg.boosted.{Role, Tier}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}


/**
  * Created by ilan on 8/25/16.
  */
class BoostedSummonerTest extends FlatSpec with BeforeAndAfter{

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
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now))
        )

        val result = BoostedSummoner.calculate(df, 0, 0, 100).collect()

        assert(result.length === 1)
    }

    "A list with one loss" should "return no results" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, false, "NA", now))
        )

        val result = BoostedSummoner.calculate(df, 0, 0, 100).collect()

        assert(result.length === 0)
    }

    "A list with a winner and a loser" should "filter out the loser" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(1,2,1,Role.TOP.roleId, false, "NA", now)
        ))

        val result = BoostedSummoner.calculate(df, 0, 0, 100).collect()
        assert(result.length === 1)
    }

    "A list with someone that hasn't played enough games" should "be filtered out" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now)
        ))

        val result = BoostedSummoner.calculate(df, 2, 0, 100).collect()
        assert(result.length === 0)
    }

    "People who haven't played enough games" should "be fitlered out" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(3,2,1,Role.TOP.roleId, true, "NA", now)
        ))

        val result = BoostedSummoner.calculate(df, 2, 0, 100).collect()
        assert(result.length === 1)
        //The one who is left is player number 2
        assert(result(0).summonerId === 2)
    }

    "A list with an old game" should "be filtered out" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", 0)
        ))

        val result = BoostedSummoner.calculate(df, 0, 1, 100).collect()
        assert(result.length === 0)
    }


    "When there are many winners they" should "be ordered by winrate desc" in {
        val df = spark.createDataset[SummonerMatch](List(
            //100%
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),

            //66%
            SummonerMatch(4,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(5,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(6,2,1,Role.TOP.roleId, false, "NA", now),

            //75%
            SummonerMatch(7,3,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(8,3,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(9,3,1,Role.TOP.roleId, false, "NA", now),
            SummonerMatch(10,3,1,Role.TOP.roleId, true, "NA", now),

            //50% -> Should be filtered out
            SummonerMatch(11,4,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(12,4,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(13,4,1,Role.TOP.roleId, false, "NA", now),
            SummonerMatch(14,4,1,Role.TOP.roleId, false, "NA", now)
        ))

        val result = BoostedSummoner.calculate(df, 1, 0, 100).as[BoostedSummoner].collect()
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

    "When the same summoner-match is encountered more than once it" should "be counted just once" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now)
        ))

        val calcResult = BoostedSummoner.calculate(df, 1, 0, 100).collect()
        assert (calcResult.length === 1)
        assert (calcResult(0).gamesPlayed === 1)

    }

    "Many matches with single chrole" should "be ordered according to rank by winrate" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(3,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(4,2,1,Role.TOP.roleId, false, "NA", now)
        ))

        val calcResult = BoostedSummoner.calculate(df, 1, 0, 100).collect()
        assert (calcResult.length === 2)
        assert (calcResult(0).rank === 1)
        assert (calcResult(1).rank === 2)
    }

    "If there are 2 or more chroles, then the ranks" should "be sorted for each role individually" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(3,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(4,2,1,Role.TOP.roleId, false, "NA", now),
            SummonerMatch(1,3,2,Role.BOTTOM.roleId, true, "NA", now)
        ))

        val calcResult = BoostedSummoner.calculate(df, 1, 0, 100).collect()
        assert (calcResult.length === 3)
        assert (calcResult(0).rank === 1)
        assert (calcResult(1).rank === 2)
        assert (calcResult(2).rank === 1)
    }

    "If 2 summoners have the same winrate for the same chrole then the results" should
        "be ordered according to games played" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(3,2,1,Role.TOP.roleId, true, "NA", now)
        ))

        val calcResult = BoostedSummoner.calculate(df, 1, 0, 100).collect()
        assert (calcResult.length === 2)

        //Summoner 1 should have rank 2
        val summoner1 = calcResult.find(row => row.summonerId == 1).get
        assert (summoner1.rank === 2)

        //And summoner 2 should have rank 1
        val summoner2 = calcResult.find(row => row.summonerId == 2).get
        assert (summoner2.rank === 1)

    }

    "When winrate and games_played are tied then the results" should "still be of different rank" in {
        //Since i arbitrarily chose the summonerId (desc) to be the tie breaker in the case that they have
        //The same winrate and games played i will check that...
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now)
        ))

        val calcResult = BoostedSummoner.calculate(df, 1, 0, 100).collect()
        assert (calcResult.length === 2)

        //Summoner 1 should have rank 2
        val summoner1 = calcResult.find(row => row.summonerId == 1).get
        assert (summoner1.rank === 2)

        //And summoner 2 should have rank 1
        val summoner2 = calcResult.find(row => row.summonerId == 2).get
        assert (summoner2.rank === 1)

    }

    "Returned results for a chrole" should "not exceed max rank" in {
        val df = spark.createDataset[SummonerMatch](List(
            SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now),

            SummonerMatch(2,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(3,2,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(4,2,1,Role.TOP.roleId, false, "NA", now),

            SummonerMatch(5,3,1,Role.TOP.roleId, true, "NA", now),
            SummonerMatch(6,3,1,Role.TOP.roleId, false, "NA", now),
            SummonerMatch(7,3,1,Role.TOP.roleId, false, "NA", now)
        ))

        val calcResult = BoostedSummoner.calculate(df, 1, 0, 2).collect()
        assert (calcResult.length === 2)

        //Summoner 1 should have rank 1
        val summoner1 = calcResult.find(row => row.summonerId == 1).get
        assert (summoner1.rank === 1)

        //And summoner 2 should have rank 2
        val summoner2 = calcResult.find(row => row.summonerId == 2).get
        assert (summoner2.rank === 2)
    }

}
