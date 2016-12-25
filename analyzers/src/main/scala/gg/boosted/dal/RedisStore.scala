package gg.boosted.dal

import gg.boosted.configuration.Configuration
import gg.boosted.posos.{LoLScore, MatchId, SummonerId}
import org.sedis._
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val pool = new Pool(new JedisPool(new JedisPoolConfig(), Configuration.getString("redis.location"), 6379, 2000))

    val summonerIdToNameKey = "summonerIdToName"
    val summonerNameTTL = Configuration.getInt("summoner.to.name.retention.period.seconds")

    val summonerIdToLOLScoreKey = "summonerIdToLOLScore"
    val summonerLOLScoreTTL = Configuration.getInt("summoner.to.lolscore.retention.period.seconds")

    val matchIdKey = "fullMatch"
    val matchIdTTL = Configuration.getInt("full.match.retention.period.seconds")

    def getSummonerName(id:SummonerId):Option[String] = {
        pool.withClient { _.get(s"$summonerIdToNameKey:${id.region}:${id.id}")}
    }

    def addSummonerName(id:SummonerId, summonerName:String):Unit = {
        pool.withClient { _.setex(s"$summonerIdToNameKey:${id.region}:${id.id}", summonerNameTTL, summonerName) }
    }

    /**
      * LoL Score uses 3 keys
      * @param id
      * @return
      */
    def getSummonerLOLScore(id:SummonerId):Option[LoLScore] = {
        pool.withClient ( client => {
            val tier = client.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:tier").getOrElse(return None)
            val division = client.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:division").getOrElse(return None)
            val leaguePoints = client.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:leaguePoints").getOrElse(return None)
            return Some(LoLScore(tier, division, leaguePoints.toInt))
        })
        return None
    }

    def addSummonerLOLScore(id:SummonerId, lolScore:LoLScore):Unit = {
        pool.withClient ( client => {
            client.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:tier", summonerLOLScoreTTL, lolScore.tier)
            client.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:division", summonerLOLScoreTTL, lolScore.division)
            client.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:leaguePoints", summonerLOLScoreTTL, lolScore.leaguePoints.toString)
        })
    }

    /**
      * Return the json representation of a full match
      * @param id
      * @return
      */
    def getMatch(id:MatchId):Option[String] = {
        pool.withClient { _.get(s"$matchIdKey:${id.region}:${id.id}")}
    }

    def addMatch(id:MatchId, matchJson:String):Unit = {
        pool.withClient { _.setex(s"$matchIdKey:${id.region}:${id.id}", matchIdTTL, matchJson) }
    }


}
