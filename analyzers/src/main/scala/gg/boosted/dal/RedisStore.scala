package gg.boosted.dal

import com.redis._

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val rc = new RedisClient("10.0.0.3", 6379)

    val summonerIdToNameKey = "summonerIdToName"

    val summonerNameTTL = 60*60*24*5 //5 days

    def getSummonerNameById(region:String, summonerId:Long):Option[String] = {
        rc.get(s"$summonerIdToNameKey:$region:$summonerId")
    }

    def addSummonerName(region:String, summonerId:Long, summonerName:String):Unit = {
        rc.setex(s"$summonerIdToNameKey:$region:$summonerId", summonerNameTTL, summonerName)
    }

}
