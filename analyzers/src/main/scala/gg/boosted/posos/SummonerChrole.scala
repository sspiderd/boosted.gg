package gg.boosted.posos

/**
  * Created by ilan on 8/26/16.
  */
case class SummonerChrole (championId:Int, roleId:Int, summonerId:Long, tier:Int, region:String, gamesPlayed:BigInt, winrate:Double, matches:Seq[Long], position:Int)
