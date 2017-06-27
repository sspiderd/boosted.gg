package gg.boosted.analyzers

import gg.boosted.maps.Items
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import gg.boosted.Application.session.implicits._
import gg.boosted.utils.spark.DistinctItems

/**
  *
  * How shall i analyzer the late items...?
  * I'm thinking, pick all the items that aren't core and see which contribute the most to the win
  * The core items were supposed to be calculated before so i'm not looking at them
  *
  * Created by ilan on 2/8/17.
  */
object LateItemsAnalyzer {

  case class ItemsSetup(items:Seq[String], winner:Boolean)

  /**
    * So i'm figuring let's try to see how much winrate i have WITH item divided but the total
    * winrate. The higher the value, the more effect this item has on the winrate
    * @param item
    * @param items
    * @return
    */
  def howMuchDoesAnItemImproveOnWinrate(item:String, items:Dataset[ItemsSetup], totalWinrate:Double):Double = {
    winrate(items.filter(itemSetup => itemSetup.items.contains(item))) / totalWinrate
  }

  /**
    * So.. ye... probably there's an easier way to calculate that...
    * @param items
    * @return
    */
  def winrate(items:Dataset[ItemsSetup]):Double = {
    items.map(row => if (row.winner) 1 else 0).agg(avg($"value")).collect()(0)(0).asInstanceOf[Double]
  }

  /**
    * Get all distinct items in the dataset
    * @param items
    * @return
    */
  def allItems(items:Dataset[ItemsSetup]):Seq[String] = {
    val di = new DistinctItems()
    items.map(_.items).agg(di($"value")).collect().head.getAs[Seq[String]](0)
  }

  def sortedByWinrateImprovements(items:Dataset[ItemsSetup]):Seq[String] = {
    val totalWinrate = winrate(items)

    val allItemss = allItems(items)


    var winImprovementMap = Map[String, Double]()
    //For each item, calculate its improvement on the winrate, return in descending order
    allItemss.foreach(item => {
      val improvement = howMuchDoesAnItemImproveOnWinrate(item, items, totalWinrate)
      winImprovementMap += (item -> improvement)
    })

    return winImprovementMap.map(_.swap).toSeq.sortBy(_._1).reverse.map(_._2)
  }

  def optimalLateItems(items:Dataset[ItemsSetup]):Seq[String] = {
    //Filter legendaries only
    val filteredLegendaries = items.map(itemSet => ItemsSetup(itemSet.items.filter(Items.isLegendary), itemSet.winner))

    sortedByWinrateImprovements(filteredLegendaries)
  }

}
