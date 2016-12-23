package gg.boosted.posos

/**
  * Created by ilan on 12/23/16.
  */
case class MatchEvent(championId:Int, role:String, summonerId:Long, region:String, eventType:String, timeStamp:Long,
                      itemId:Int, skillSlot:Int, winLose:Boolean)
