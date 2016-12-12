package gg.boosted.dal

import com.redis._
import gg.boosted.posos.{LoLScore, SummonerId}
import gg.boosted.riotapi.Region

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val rc = new RedisClient("10.0.0.3", 6379)

    val summonerIdToNameKey = "summonerIdToName"
    val summonerNameTTL = 60*60*24*5 //5 days

    val summonerIdToLOLScoreKey = "summonerIdToLOLScore"
    val summonerLOLScoreTTL = 60*60*24 // 1 day

    def getSummonerName(id:SummonerId):Option[String] = {
        rc.get[String](s"$summonerIdToNameKey:${id.region}:${id.id}")
    }

    def addSummonerName(id:SummonerId, summonerName:String):Unit = {
        rc.setex(s"$summonerIdToNameKey:${id.region}:${id.id}", summonerNameTTL, summonerName)
    }

    /**
      * LoL Score uses 3 keys
      * @param id
      * @return
      */
    def getSummonerLOLScore(id:SummonerId):Option[LoLScore] = {
        val tier = rc.get[String](s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:tier").getOrElse(return None)
        val division = rc.get[String](s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:division").getOrElse(return None)
        val leaguePoints = rc.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:leaguePoints").getOrElse(return None)
        return Some(LoLScore(tier, division, leaguePoints.toInt))

    }

    def addSummonerLOLScore(id:SummonerId, lolScore:LoLScore):Unit = {
        rc.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:tier", summonerLOLScoreTTL, lolScore.tier)
        rc.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:division", summonerLOLScoreTTL, lolScore.division)
        rc.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:leaguePoints", summonerLOLScoreTTL, lolScore.leaguePoints)
    }

}
