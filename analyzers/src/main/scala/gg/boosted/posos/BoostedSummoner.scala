package gg.boosted.posos

/**
  * Created by ilan on 12/23/16.
  */
case class BoostedSummoner(championId:Int, role:String, summonerId:Long, region:String, gamesPlayed:Long,
                           winrate:Double, matches:Seq[Long], rank:Int)
