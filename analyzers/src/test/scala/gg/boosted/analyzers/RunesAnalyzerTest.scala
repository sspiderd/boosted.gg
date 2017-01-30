package gg.boosted.analyzers

import java.util.Date

import gg.boosted.Role
import gg.boosted.posos.{SummonerMatch, SummonerMatchSummary}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 1/29/17.
  */
class RunesAnalyzerTest extends FlatSpec with BeforeAndAfter{

  private val master = "local[*]"
  private val appName = "RunesAnalyzerTest"

  private val now = new Date().getTime ;

  private val spark:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._

  "Blah blah" should "blah blah" in {
//    val df = spark.createDataset[SummonerMatchSummary](List(
//      SummonerMatchSummary()
//    )
//    val df = spark.createDataset[SummonerMatch](List(
//      SummonerMatch(1,1,1,Role.TOP.roleId, true, "NA", now))
//    )
//
//    val result = BoostedSummonersAnalyzer.findBoostedSummoners(df, 0, 0, 100).collect()
//
//    assert(result.length === 1)
  }

}
