package gg.boosted.posos

/**
  * Created by ilan on 12/29/16.
  */
case class SummonerMatchSummary(matchId:Long, summonerId:Long, championId:Int, roleId:Int, winner:Boolean, region: String, date: Long,
                                itemsBought:List[Int])
