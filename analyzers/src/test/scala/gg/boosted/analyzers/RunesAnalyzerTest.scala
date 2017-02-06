package gg.boosted.analyzers

import java.util.Date

import gg.boosted.analyzers.RunesAnalyzer.{Rune, RuneSetup}
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

  "A single set of runes that wins all the time" should "be returned" in {

    val runes = Seq[Rune](
      Rune("1", "red", 9, 3),
      Rune("2", "yellow", 9, 3),
      Rune("3", "blue", 9, 3),
      Rune("4", "black", 3, 3))

    val runeSetup = RuneSetup(runes, true)

    val ds = spark.createDataset[RuneSetup](List(runeSetup))
    val result = RunesAnalyzer.optimalRunes(ds, 1)

    assert (result.contains("1"))
    assert (result.contains("2"))
    assert (result.contains("3"))
    assert (result.contains("4"))


    assert (result("1") === 9)
    assert (result("2") === 9)
    assert (result("3") === 9)
    assert (result("4") === 3)
  }

  "The rune setup with the highest winrate" should "be the one chosen" in {
    val runes1 = Seq[Rune](
      Rune("1", "red", 9, 3),
      Rune("2", "yellow", 9, 3),
      Rune("3", "blue", 9, 3),
      Rune("4", "black", 3, 3))

    val runes2 = Seq[Rune](
      Rune("5", "red", 9, 3),
      Rune("6", "yellow", 9, 3),
      Rune("7", "blue", 9, 3),
      Rune("8", "black", 3, 3))

    val runeSetup1 = RuneSetup(runes1, true)
    val runeSetup2 = RuneSetup(runes2, false)
    val runeSetup3 = RuneSetup(runes1, true)

    //Runes1 has the highest winrate
    val ds = spark.createDataset[RuneSetup](Seq(runeSetup1, runeSetup2, runeSetup3))
    val result = RunesAnalyzer.optimalRunes(ds, 1)

    assert (result.contains("1"))
    assert (result.contains("2"))
    assert (result.contains("3"))
    assert (result.contains("4"))

    assert (result("1") === 9)
    assert (result("2") === 9)
    assert (result("3") === 9)
    assert (result("4") === 3)
  }


  "The more runes of the same type" should "be chosen" in {
    val runes = Seq[Rune](
      Rune("1", "red", 5, 3),
      Rune("11", "red", 4, 3),
      Rune("2", "yellow", 5, 3),
      Rune("22", "yellow", 4, 3),
      Rune("3", "blue", 5, 3),
      Rune("33", "blue", 4, 3),
      Rune("4", "black", 2, 3))
      Rune("44", "black", 1, 3)

    val runeSetup = RuneSetup(runes, true)

    val ds = spark.createDataset[RuneSetup](List(runeSetup))
    val result = RunesAnalyzer.optimalRunes(ds, 1)

    assert (result.contains("1"))
    assert (result.contains("2"))
    assert (result.contains("3"))
    assert (result.contains("4"))

    assert (result("1") === 9)
    assert (result("2") === 9)
    assert (result("3") === 9)
    assert (result("4") === 3)
  }



}
