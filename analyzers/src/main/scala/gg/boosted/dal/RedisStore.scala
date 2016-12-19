package gg.boosted.dal

import com.redis._
import gg.boosted.configuration.Configuration
import gg.boosted.posos.{LoLScore, SummonerId}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.util.{Failure, Success, Try}

/**
  * Created by ilan on 9/1/16.
  */
object RedisStore {

    val rc = new RedisClient(Configuration.getString("redis.location"), 6379)

    private val jedisPool = new JedisPool(new JedisPoolConfig, Configuration.getString("redis.location"))

    val summonerIdToNameKey = "summonerIdToName"
    val summonerNameTTL = Configuration.getInt("summoner.to.name.retention.period")

    val summonerIdToLOLScoreKey = "summonerIdToLOLScore"
    val summonerLOLScoreTTL = Configuration.getInt("summoner.to.lolscore.retention.period")

    val summonersProcessedKey = "summonersProcessed"

    def cleanly[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
        try {
            Success(doWork(resource))
        } catch {
            case e: Exception => Failure(e)
        }
        finally {
            try {
                if (resource != null) {
                    cleanup(resource)
                }
            } catch {
                case e: Exception => println(e) // should be logged
            }
        }
    }


    def getSummonerName(id:SummonerId):Option[String] = {
        cleanly(jedisPool.getResource)(_.close()) {_.get(s"$summonerIdToNameKey:${id.region}:${id.id}")} match {
            case Success(v) => Option(v)
            case Failure(e) => None
        }

        //rc.get[String](s"$summonerIdToNameKey:${id.region}:${id.id}")
    }

    def addSummonerName(id:SummonerId, summonerName:String):Unit = {
        cleanly(jedisPool.getResource)(_.close()) {_.setex(s"$summonerIdToNameKey:${id.region}:${id.id}", summonerNameTTL, summonerName)}

        //rc.setex(s"$summonerIdToNameKey:${id.region}:${id.id}", summonerNameTTL, summonerName)
    }

    /**
      * LoL Score uses 3 keys
      * @param id
      * @return
      */
    def getSummonerLOLScore(id:SummonerId):Option[LoLScore] = {
        cleanly(jedisPool.getResource)(_.close()) {conn =>
            val tier = Option(conn.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:tier")).getOrElse(return None)
            val division = Option(conn.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:division")).getOrElse(return None)
            val leaguePoints = Option(conn.get(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:leaguePoints")).getOrElse(return None)
            return Some(LoLScore(tier, division, leaguePoints.toInt))
        }
        return None
    }

    def addSummonerLOLScore(id:SummonerId, lolScore:LoLScore):Unit = {
        cleanly(jedisPool.getResource)(_.close()) { conn =>
            conn.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:tier", summonerLOLScoreTTL, lolScore.tier)
            conn.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:division", summonerLOLScoreTTL, lolScore.division)
            conn.setex(s"$summonerIdToLOLScoreKey:${id.region}:${id.id}:leaguePoints", summonerLOLScoreTTL, lolScore.leaguePoints.toString)
        }
    }

}
