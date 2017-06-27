package gg.boosted.analyzers

import gg.boosted.Application.session.implicits._
import org.apache.spark.sql.Dataset

/**
  *
  * Should probably be run on the whole cluster
  *
  * Created by ilan on 2/5/17.
  */
object StartingItemsAnalyzer {

  case class StartingItemsSetup(startingItems:Seq[String], winner:Boolean)

  /**
    * Returns the ids of the items that you should start with
    * @param ds
    * @return
    */
  def optimalStartingItems(ds:Dataset[StartingItemsSetup]):Seq[String] = {
    ds.map(sms => StartingItemsSetup(sms.startingItems.sorted, sms.winner)).createOrReplaceTempView("StartingItems")
    ds.sqlContext.sql(
      s"""
         |SELECT startingItems, mean(if(winner=true,1,0)) as winrate
         |FROM StartingItems
         |GROUP BY startingItems
         |ORDER BY winrate DESC
       """.stripMargin).map(_.getSeq[String](0)).head()
  }
}
