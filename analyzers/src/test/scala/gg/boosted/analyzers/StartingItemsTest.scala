package gg.boosted.analyzers

import java.util.Date

import gg.boosted.analyzers.StartingItemsAnalyzer.StartingItemsSetup
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.apache.spark.sql.functions._
import gg.boosted.Application.session.implicits._

/**
  * Created by ilan on 2/6/17.
  */
class StartingItemsTest extends FlatSpec with BeforeAndAfter{

  private val master = "local[*]"
  private val appName = "StartingItemsAnalyzerTest"

  private val now = new Date().getTime ;

  private val spark:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._

  "A single starting items list" should "return itself" in {
    val ds = spark.createDataset[StartingItemsSetup](Seq(
      StartingItemsSetup(Seq[String]("a", "b"), true)
    ))
    val result = StartingItemsAnalyzer.optimalStartingItems(ds)
    assert (result.size === 2)
    assert (result(0) === "a")
    assert (result(1) === "b")
  }

  "starting items" should "be sorted when tested" in {
    val ds = spark.createDataset[StartingItemsSetup](Seq(
      StartingItemsSetup(Seq[String]("a", "b"), false),
      StartingItemsSetup(Seq[String]("b", "a"), true)
    ))
    val result = StartingItemsAnalyzer.optimalStartingItems(ds)
    assert (result.size === 2)
    assert (result(0) === "a")
    assert (result(1) === "b")
  }

  "The items with the highest winrate" should "be chosen" in {
    val ds = spark.createDataset[StartingItemsSetup](Seq(
      StartingItemsSetup(Seq[String]("a", "b"), true),
      StartingItemsSetup(Seq[String]("b", "c"), true),
      StartingItemsSetup(Seq[String]("b", "c"), false)
    ))
    val result = StartingItemsAnalyzer.optimalStartingItems(ds)
    assert (result.size === 2)
    assert (result(0) === "a")
    assert (result(1) === "b")
  }


}
