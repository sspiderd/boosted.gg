package gg.boosted.analyzers

import gg.boosted.analyzers.LateItemsAnalyzer.ItemsSetup
import gg.boosted.maps.Items
import gg.boosted.utils.spark.{DistinctItems, Mods}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.avg
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 2/8/17.
  */
class LateItemsTest extends FlatSpec with BeforeAndAfter{

  private val master = "local[*]"
  private val appName = "MasteriesAnalyzerTest"

  private val spark:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._

  "Items" should "return the correct winrate" in {
    val ds = spark.createDataset(Seq[ItemsSetup](
      ItemsSetup(Seq("a"), true),
      ItemsSetup(Seq("b"), true),
      ItemsSetup(Seq("c"), false)
    ))

    assert (LateItemsAnalyzer.winrate(ds) === 2/3.0)
  }

  "All items in the dataset" should "be returned" in {
    val ds = spark.createDataset(Seq[ItemsSetup](
      ItemsSetup(Seq("a", "d"), true),
      ItemsSetup(Seq("b", "e", "f"), true),
      ItemsSetup(Seq("c"), false)
    ))

    val allItems = LateItemsAnalyzer.allItems(ds)
    assert (allItems.contains("a"))
    assert (allItems.contains("b"))
    assert (allItems.contains("c"))
    assert (allItems.contains("d"))
    assert (allItems.contains("e"))
    assert (allItems.contains("f"))
  }

  "All items in the dataset" should "be returned once" in {
    val ds = spark.createDataset(Seq[ItemsSetup](
      ItemsSetup(Seq("a", "b"), true),
      ItemsSetup(Seq("b", "c"), false)
    ))

    val allItems = LateItemsAnalyzer.allItems(ds)
    assert (allItems.filter(_ == "a").size === 1)
    assert (allItems.filter(_ == "b").size === 1)
    assert (allItems.filter(_ == "c").size === 1)
    assert (allItems.size === 3)
  }

  "Items in the dataset" should "be returned by winrate improvements" in {
    val ds = spark.createDataset(Seq[ItemsSetup](
      ItemsSetup(Seq("a", "b"), true),
      ItemsSetup(Seq("b", "c"), false)
    ))

    val allItems = LateItemsAnalyzer.sortedByWinrateImprovements(ds)

    assert (allItems.size === 3)
    assert (allItems(0) === "a")
    assert (allItems(1) === "b")
    assert (allItems(2) === "c")

  }

}
