package gg.boosted.dal

import com.redis._
import gg.boosted.configuration.Configuration
import gg.boosted.posos.{LoLScore, SummonerId}

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val rc = new RedisClient(Configuration.getString("redis.location"), 6379)

    val summonerIdToNameKey = "summonerIdToName"
    val summonerNameTTL = Configuration.getLong("summoner.to.name.retention.period")

    val summonerIdToLOLScoreKey = "summonerIdToLOLScore"
    val summonerLOLScoreTTL = Configuration.getLong("summoner.to.lolscore.retention.period")

    val summonersProcessedKey = "summonersProcessed"

    def getSummonerName(id:SummonerId):Option[String] = synchronized {
        rc.get[String](s"$summonerIdToNameKey:${id.region}:${id.id}")
    }

    def addSummonerName(id:SummonerId, summonerName:String):Unit = synchronized {
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

    def wasSummonerProcessedAlready(summonerId:SummonerId): Boolean = {
        true
        //rc.smembers(s"${summonersProcessedKey}:${summonerId.region.toString}").contains(summonerId.id.toString)
    }

}
