package gg.boosted.maps

import gg.boosted.posos.LoLScore
import net.rithms.riot.api.{ApiConfig, RiotApi}
import net.rithms.riot.constant.Region
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by ilan on 9/9/16.
  */
@Deprecated
object SummonerIdToLoLScore {

    val log = LoggerFactory.getLogger(getClass)

    val map = collection.mutable.HashMap.empty[String, mutable.HashMap[Long,LoLScore]]

    def +=(region:String, summonerId:Long, lolScore:LoLScore):Unit = {
        map.get(region) match {
            case Some(m) => m += (summonerId -> lolScore)
            case None => map += (region ->  collection.mutable.HashMap(summonerId -> lolScore))
        }
    }

    def apply(region:String, summonerId:Long):LoLScore = {
        map(region)(summonerId)
    }

    /**
      *
      * @param regionToSummonerIds
      * @return a map of region to a map of (summonerId, summonerName)
      */
    def populateLoLScoresByIds(regionToSummonerIds:Map[String, Array[Long]]):Unit = {

        val config = new ApiConfig()
        config.setKey(System.getenv("RIOT_API_KEY"))
        val riot = new RiotApi(config)
        regionToSummonerIds.foreach(region => {

            //The map should be cached. that is, if i already retrieved an id -> name before, i don't want to do it again
            val knownIds = map.get(region._1) match {
                case None => Set.empty[Long]
                case Some(m) => m.keySet
            }

            val requestedIds = region._2.toSet

            val unknownIds = requestedIds.diff(knownIds)

            //FUCKING SCALA! here i have to convert both the list and the "Long" to conform to java, and then back to scala
            import collection.JavaConverters._
            //Retrieve summoner names for all unknown ids
            val summonerIdToLeagueMap = riot.getExtendedLeagueEntryBySummoners(Region.valueOf(region._1), unknownIds.toList.map(Long.box).asJava).asScala

            //And put the in the map (cache)
            for ((summonerId, summonerLeagueEntry) <- summonerIdToLeagueMap) {
                +=(region._1,
                    summonerId.toLong,
                    LoLScore(summonerLeagueEntry.tier, summonerLeagueEntry.division, summonerLeagueEntry.leaguePoints))
            }
            log.debug(s"Mapped additional ${summonerIdToLeagueMap.size} summoner ids to league entries for region '${region._1}'")
        })
    }
}
