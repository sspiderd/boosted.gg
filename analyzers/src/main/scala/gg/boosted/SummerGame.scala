package gg.boosted

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Created by ilan on 8/13/16.
  */
case class SummonerGame(matchId:Long, summonerId:Long, championId:Int, role:Role, winner:Boolean)

object SummonerGame {
  def apply(json: String): SummonerGame = {
    implicit val formats = DefaultFormats

    val parsed = parse(json)
    SummonerGame.apply(
      (parsed \ "matchId").extract[Long],
      (parsed \ "summonerId").extract[Long],
      (parsed \ "championId").extract[Int],
      (parsed \ "role").extract[Role],
      (parsed \ "winner").extract[Boolean])
  }
}