package gg.boosted.dal

import com.redis._

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val rc = new RedisClient("10.0.0.3", 6379)

    val summonerIdToNameMap = "summonerIdToName"

    def getSummonerNameById(region:String, summonerId:Long):Option[String] = {
        rc.hget(s"$summonerIdToNameMap-$region", summonerId)
    }

    def addSummonerName(region:String, summonerId:Long, summonerName:String):Unit = {
        rc.hset(s"$summonerIdToNameMap-$region", summonerId, summonerName)
    }

}
