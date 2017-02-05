package gg.boosted.analyzers

import gg.boosted.posos.SummonerMatchSummary
import org.apache.spark.sql.Dataset

/**
  *
  * Should probably be run on the whole cluster
  *
  * Created by ilan on 2/5/17.
  */
object StartingItemsAnalyzer {

  /**
    * Returns the ids of the items that you should start with
    * @param ds
    * @return
    */
  def optimalStartingItems(ds:Dataset[SummonerMatchSummary]):Seq[String] = {
    ds.createOrReplaceTempView("StartingItems")
    ds.sqlContext.sql(
      s"""
         |SELCET firstItemsBought, mean(if(winner=true,1,0)) as winrate
         |FROM StartingItems
         |GROUP BY firstItemsBought
         |ORDER BY winrate DESC
       """.stripMargin).map(_.getSeq[String](0)).head()
  }
}
