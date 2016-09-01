package gg.boosted.dal

import com.redis._

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val rc = new RedisClient("10.0.0.3", 6379)

    val summonerIdToNameMap = "summonerIdToName"

    def getSummonerNameById(summonerId:Long):Option[String] = {
        rc.hget(summonerIdToNameMap, summonerId)
    }

    def addSummonerName(summonerId:Long, summonerName:String):Unit = {
        rc.hset(summonerIdToNameMap, summonerId, summonerName)
    }

}
