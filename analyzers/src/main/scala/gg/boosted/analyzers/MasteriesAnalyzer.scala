package gg.boosted.analyzers

import gg.boosted.maps.Masteries
import gg.boosted.riotapi.dtos.`match`.Mastery
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import gg.boosted.Application.session.implicits._

/**
  * Created by ilan on 2/5/17.
  */
object MasteriesAnalyzer {

  case class MasterySetup(masteries: Seq[Mastery], winner: Boolean)

  private def standarize(ds: Dataset[MasterySetup]): Dataset[MasterySetup] = {
    def max(m1: Mastery, m2: Mastery): Mastery = if (m1.rank > m2.rank) m1 else m2
    ds.map(setup => {

      val treeGroups = setup.masteries.groupBy(mastery => Masteries.tree(mastery.masteryId))
      val heightGroups = treeGroups.map(group => (group._1, group._2.groupBy(mastery => Masteries.height(mastery.masteryId))))

      //Now it looks like this:
      //(tree1 -> ( height1 -> (mastery11, mastery12)
      //(         ( height2 -> (mastery21, mastery22)
      //(.
      //(.
      //(tree2 -> ( height1 -> mastery11, mastery12)
      //(.

      //So now we eliminate the (mastery11, mastery12) part. the mastery with the higher rank wins
      val standardHeightGroups = heightGroups.map(group => (group._1, group._2.map(height => (height._1, height._2.reduce(max)))))

      val sortedMasteries = standardHeightGroups.toSeq.flatMap(_._2.values).sortBy(_.masteryId)
      MasterySetup(sortedMasteries, setup.winner)

    })
  }

  def optimalMasteries(ds:Dataset[MasterySetup]):Seq[String] = {
    standarize(ds).createOrReplaceTempView("OptimalMasteries")
    ds.sqlContext.sql(
      s"""
         |SELECT masteries, mean(if(winner=true,1,0)) as winrate
         |FROM OptimalMasteries
         |GROUP BY maseries
         |ORDER BY winrate DESC
       """.stripMargin).map(_.getSeq[String](0)).head()
  }


}
