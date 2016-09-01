package gg.boosted.posos

/**
  * Created by ilan on 8/26/16.
  */
case class SummonerChrole (championId:Int, roleId:Int, summonerId:Long, summonerName:String, region:String, tier:Int, gamesPlayed:Long, winrate:Double, matches:Seq[Long])
