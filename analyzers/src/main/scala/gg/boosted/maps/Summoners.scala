package gg.boosted.maps

import gg.boosted.dal.RedisStore
import gg.boosted.posos.{LoLScore, SummonerId}
import gg.boosted.riotapi.RiotApi
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  *
  * Returns stuff about summoners
  *
  * Created by ilan on 12/8/16.
  */
@Deprecated
object Summoners {

    val log = LoggerFactory.getLogger(Summoners.getClass)

    /**
      * Returns a map of id->name for the given ids
      * @param ids
      * @return
      */
    def getNames(ids:Seq[SummonerId]):Map[SummonerId, String] = {
        val map = scala.collection.mutable.HashMap.empty[SummonerId, String]

        if (ids.size == 0) {
            map.toMap
        }
        //We search redis for the id. if it is there, we simply put it in the map, if it isn't we add to the list of
        //the unknowns
        val unknowns = new ListBuffer[SummonerId]

        ids.foreach(id => {
            RedisStore.getSummonerName(id) match {
                case Some(name) => map(id) = name //We know this name, put it in the map
                case None => unknowns += id //We've never heard of that dude (well, for the past TTL days, anyway
            }
        })

        log.debug("Found {} summoner names already in store", map.size)

        //Group by region and retrieve the names
        unknowns.groupBy(_.region).foreach { case(region, ids) => {
            val api = new RiotApi(region)

            //Yes, fucking scala is forcing me the stupid unreadable line below
            import collection.JavaConverters._
            val idsToNamesMap = api.getSummonerNamesByIds(ids.map(_.id).map(Long.box):_*).asScala

            idsToNamesMap.foreach { case(id, name) => {
                log.debug("Adding new summoner to redis store {} -> {}", id, name, "")
                RedisStore.addSummonerName(SummonerId(id, region), name)
                map(SummonerId(id, region)) = name
            }
            }
        }}

        return map.toMap

    }
    /**
      * Returns a map of id->score for the given ids
      * @param ids
      * @return
      */
    def getLOLScores(ids:Seq[SummonerId]):Map[SummonerId, LoLScore] = {
        val map = scala.collection.mutable.HashMap.empty[SummonerId, LoLScore]

        if (ids.size == 0) {
            map.toMap
        }
        //We search redis for the id. if it is there, we simply put it in the map, if it isn't we add to the list of
        //the unknowns
        val unknowns = new ListBuffer[SummonerId]

        ids.foreach(id => {
            RedisStore.getSummonerLOLScore(id) match {
                case Some(name) => map(id) = name //We know this name, put it in the map
                case None => unknowns += id //We've never heard of that dude (well, for the past TTL days, anyway
            }
        })

        log.debug("Found {} summoner lol scores already in store", map.size)

        //Group by region and retrieve the names
        unknowns.groupBy(_.region).foreach { case(region, ids) => {
            val api = new RiotApi(region)

            //Yes, fucking scala is forcing me the stupid unreadable line below
            import collection.JavaConverters._
            val idsToScoresMap = api.getLeagueEntries(ids.map(_.id).map(Long.box):_*).asScala

            idsToScoresMap.foreach { case(id, entry) => {
                val summonerId = SummonerId(id, region)
                val lolScore = LoLScore(entry.tier, entry.division, entry.leaguePoints)
                log.debug(s"Adding new summoner lol score to redis store ${id} -> ${lolScore.leaguePoints}")
                RedisStore.addSummonerLOLScore(summonerId, lolScore)
                map(summonerId) = lolScore
            }
            }
        }}

        return map.toMap

    }

}
