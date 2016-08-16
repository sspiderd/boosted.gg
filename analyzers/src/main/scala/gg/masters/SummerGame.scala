package gg.masters

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Created by ilan on 8/13/16.
  */
case class SummonerGame(matchId:Long, summonerId:Long, championId:Int, role:String, winner:Boolean)

object SummonerGame {
  def apply(json: String): SummonerGame = {
    implicit val formats = DefaultFormats

    val parsed = parse(json)
    SummonerGame.apply(
      (parsed \ "matchId").extract[Long],
      (parsed \ "summonerId").extract[Long],
      (parsed \ "championId").extract[Int],
      (parsed \ "role").extract[String],
      (parsed \ "winner").extract[Boolean])
  }
}