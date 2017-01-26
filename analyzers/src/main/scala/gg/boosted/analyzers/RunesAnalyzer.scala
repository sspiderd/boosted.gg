package gg.boosted.analyzers

import gg.boosted.maps.Runes
import gg.boosted.posos.SummonerMatchSummary
import org.apache.spark.sql.Dataset

/**
  * Created by ilan on 1/26/17.
  */
object RunesAnalyzer {

  case class Rune(id: String, color: String, amount: Int, tier: Int)


  /**
    * Returns a map of the optimal rune combination for the given dataset
    * @param ds
    * @return
    */
  def optimalRunes(ds: Dataset[SummonerMatchSummary]): Map[String, Int] = {
    ds.map(row => (row.runes, row.winner)).map(row => {
      var runeSet = Set[Rune]()
      row._1.foreach(rune => {
        val runeDef = Runes.byId(rune._1)
        runeSet += Rune(rune._1, runeDef.`type`, rune._2, runeDef.tier)
      })

      //Currently i'm keeping it simple
      //Find the most prominent rune in each category

      def max(r1:Rune, r2:Rune):Rune = if (r1.amount > r2.amount) r1 else r2

      //http://alvinalexander.com/scala/scala-use-reduceleft-get-max-min-from-collection
      runeSet.filter(_.tier == 3).groupBy(_.color).map(_._2)
    })
  }

}
