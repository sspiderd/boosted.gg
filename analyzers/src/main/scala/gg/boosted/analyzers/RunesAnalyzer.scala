package gg.boosted.analyzers

import gg.boosted.Application.session.implicits._
import gg.boosted.maps.Runes
import gg.boosted.posos.SummonerMatchSummary
import org.apache.spark.sql.Dataset

/**
  * Created by ilan on 1/26/17.
  */
object RunesAnalyzer {

  case class Rune(id: String, color: String, amount: Int, tier: Int)

  case class RuneSetup(runes: Seq[Rune], winner: Boolean)

  object RuneSetup {
    def apply(sms:SummonerMatchSummary):RuneSetup = {
      val runes:List[Rune] = sms.runes.map {case (id, amount) => {
        val runeDef = Runes.byId(id)
        Rune(id, runeDef.`type`, amount, runeDef.tier)
      }}.toList
      RuneSetup(runes, sms.winner)
    }
  }

  //Representing a rune setup where only one type of rune is allowed per color
  case class SingleRuneSetup(red: String, yellow: String, blue: String, black: String, winner:Boolean)

  object SingleRuneSetup {
    def apply(runeSetup:RuneSetup):SingleRuneSetup ={
      var red, yellow, blue, black = ""
      runeSetup.runes.foreach(rune => rune.color match {
        case "red" => red = rune.id
        case "yellow" => yellow = rune.id
        case "blue" => blue = rune.id
        case "black" => black = rune.id
      })
      SingleRuneSetup(red, yellow, blue, black, runeSetup.winner)
    }
  }


  private def standarize(ds: Dataset[RuneSetup]): Dataset[RuneSetup] = {
    def max(r1: Rune, r2: Rune): Rune = if (r1.amount > r2.amount) r1 else r2
    ds.map(setup => {

      RuneSetup(setup.runes.groupBy(_.color).values.map(_.reduce(max)).toSeq.map(rune => Rune(rune.id, rune.color,
        rune.color match {
          case "black" => 3
          case _ => 9
        },
        rune.tier)), setup.winner)
    })
  }

  private def singleRuness(dataset: Dataset[RuneSetup]):Dataset[SingleRuneSetup] = {
    val standarizedRunes = standarize(dataset)
    standarizedRunes.map(runeSetup => {
      val red = runeSetup.runes.filter(_.color == "red").head.id
      val yellow = runeSetup.runes.filter(_.color == "yellow").head.id
      val blue = runeSetup.runes.filter(_.color == "blue").head.id
      val black = runeSetup.runes.filter(_.color == "black").head.id
      SingleRuneSetup(red, yellow, blue, black, runeSetup.winner)
    })
  }

  /**
    * Returns a map of the optimal rune combination for the given dataset
    *
    * @param ds
    * @return
    */
  def optimalRunes(ds: Dataset[RuneSetup], runesSetupSeenAtLeastXTimes: Int): Map[String, Int] = {

    //Make a dataset of type (9 red, 9 yellow, 9 blue, 3 black) of the same type
    val singleRunes = singleRuness(ds)
    singleRunes.createOrReplaceTempView("RunesSetup")
    val optimal = singleRunes.sqlContext.sql(
      s"""
        |SELECT red, yellow, blue, black, mean(if (winner=true,1,0)) as winrate, count(*) as gamesPlayed
        |FROM RunesSetup
        |GROUP BY red, yellow, blue, black
        |HAVING gamesPlayed >= ${runesSetupSeenAtLeastXTimes}
        |ORDER BY winrate DESC
      """.stripMargin).head()
    Map[String, Int](optimal.getString(0) -> 9,
      optimal.getString(1) -> 9,
      optimal.getString(2) -> 9,
      optimal.getString(3) -> 3)
  }

  def main(args: Array[String]): Unit = {
    val runes = List(Rune("1", "a", 1, 3),
      Rune("2", "a", 2, 3),
      Rune("3", "b", 2, 4))

    val setup = RuneSetup(runes, true)

    def max(r1: Rune, r2: Rune): Rune = if (r1.amount > r2.amount) r1 else r2

    val runeSetup = RuneSetup(setup.runes.groupBy(_.color).map(_._2).map(_.reduce(max(_, _))).toSeq.map(rune => Rune(rune.id, rune.color, 9, rune.tier)), true)
    println(runeSetup)
  }

}
