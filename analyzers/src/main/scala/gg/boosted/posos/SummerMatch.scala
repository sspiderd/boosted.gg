package gg.boosted.posos

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Created by ilan on 8/13/16.
  */
case class SummonerMatch(matchId:Long, summonerId:Long, championId:Int, roleId:Int, winner:Boolean, region: String, date: Long, tier: Int)

object SummonerMatch {
  def apply(json: String): SummonerMatch = {
    implicit val formats = DefaultFormats

    val parsed = parse(json)
    SummonerMatch.apply(
      (parsed \ "matchId").extract[Long],
      (parsed \ "summonerId").extract[Long],
      (parsed \ "championId").extract[Int],
      (parsed \ "roleId").extract[Int],
      (parsed \ "winner").extract[Boolean],
      (parsed \ "region").extract[String],
      (parsed \ "date").extract[Long],
      (parsed \ "tier").extract[Int])
  }
}