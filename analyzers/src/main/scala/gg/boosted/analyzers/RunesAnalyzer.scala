package gg.boosted.analyzers

import gg.boosted.maps.Runes
import gg.boosted.posos.SummonerMatchSummary
import gg.boosted.riotapi.dtos.RuneDef
import org.apache.spark.sql.Dataset

/**
  * Created by ilan on 1/26/17.
  */
object RunesAnalyzer {

  case class Rune(id: String, color: String, amount: Int, tier: Int)

  case class RuneSetup(runes: Iterable[Rune], winner: Boolean)

  object RuneSetup {
    def apply(sms:SummonerMatchSummary):RuneSetup = {
      val runes:Iterable[Rune] = sms.runes.map {case (id, amount) => {
        val runeDef = Runes.byId(id)
        Rune(id, runeDef.`type`, amount, runeDef.tier)
      }}
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

      RuneSetup(setup.runes.groupBy(_.color).map(_._2).map(_.reduce(max(_, _))).map(rune => Rune(rune.id, rune.color,
        rune.color match {
          case "black" => 3
          case _ => 9
        },
        rune.tier)), setup.winner)
    })
  }

  /**
    * Returns a map of the optimal rune combination for the given dataset
    *
    * @param ds
    * @return
    */
  def optimalRunes(ds: Dataset[RuneSetup]): Map[String, Int] = {


    val standardRunes = standarize(ds)



  }

  def main(args: Array[String]): Unit = {
    val runes = List(Rune("1", "a", 1, 3),
      Rune("2", "a", 2, 3),
      Rune("3", "b", 2, 4))

    val setup = RuneSetup(runes, true)

    def max(r1: Rune, r2: Rune): Rune = if (r1.amount > r2.amount) r1 else r2

    val runeSetup = RuneSetup(setup.runes.groupBy(_.color).map(_._2).map(_.reduce(max(_, _))).map(rune => Rune(rune.id, rune.color, 9, rune.tier)), true)
    println(runeSetup)
  }

}
