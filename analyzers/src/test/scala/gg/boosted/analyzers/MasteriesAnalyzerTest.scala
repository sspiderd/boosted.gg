package gg.boosted.analyzers

import java.util.Date

import gg.boosted.analyzers.MasteriesAnalyzer.{Mastery, MasterySetup}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 2/6/17.
  */
class MasteriesAnalyzerTest extends FlatSpec with BeforeAndAfter{

  private val master = "local[*]"
  private val appName = "MasteriesAnalyzerTest"

  private val spark:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._

  "Sending masteries" should "return themselves" in {
    val masteries = Seq[Mastery](
      Mastery("6111", 5),
      Mastery("6121", 5)
    )
    val masterySetup = MasterySetup(masteries, true)

    val ds = spark.createDataset[MasterySetup](Seq[MasterySetup](masterySetup))

    val result = MasteriesAnalyzer.optimalMasteries(ds)

    assert (result.size === 2)
    assert (result(0) === "6111")
    assert (result(1) === "6121")
  }

  "masteries" should "be returned sorted" in {
    val masteries = Seq[Mastery](
      Mastery("6121", 5),
      Mastery("6111", 5)
    )
    val masterySetup = MasterySetup(masteries, true)

    val ds = spark.createDataset[MasterySetup](Seq[MasterySetup](masterySetup))

    val result = MasteriesAnalyzer.optimalMasteries(ds)

    assert (result.size === 2)
    assert (result(0) === "6111")
    assert (result(1) === "6121")
  }

  "masteries" should "be normalized according to highest rank" in {
    val masteries = Seq[Mastery](
      Mastery("6111", 2),
      Mastery("6112", 3)
    )
    val masterySetup = MasterySetup(masteries, true)

    val ds = spark.createDataset[MasterySetup](Seq[MasterySetup](masterySetup))

    val result = MasteriesAnalyzer.optimalMasteries(ds)

    assert (result.size === 1)
    assert (result(0) === "6112")
  }

  "different mastery trees" should "be returned according to highest winrate" in {
    val masteries1 = Seq[Mastery](
      Mastery("6111", 5),
      Mastery("6121", 5)
    )
    val masteries2 = Seq[Mastery](
      Mastery("6211", 5),
      Mastery("6221", 5)
    )
    val masterySetup1 = MasterySetup(masteries1, true)
    val masterySetup2 = MasterySetup(masteries1, false)
    val masterySetup3 = MasterySetup(masteries2, true)

    val ds = spark.createDataset[MasterySetup](Seq[MasterySetup](masterySetup1, masterySetup2, masterySetup3))

    val result = MasteriesAnalyzer.optimalMasteries(ds)

    assert (result.size === 2)
    assert (result(0) === "6211")
    assert (result(1) === "6221")
  }


}
