package gg.boosted.dal

import gg.boosted.{Champions, Role, Tier}
import gg.boosted.analyzers.BoostedSummonersChrolesToWR
import net.rithms.riot.api.{ApiConfig, RiotApi}
import net.rithms.riot.constant.Region
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 9/1/16.
  */
case class BoostedEntity (
    champion:String,
    role:String,
    summonerId:Long,
    summonerName:String,
    region:String,
    tier:String,
    gamesPlayed:Long,
    winrate:Double,
    matches:List[Long],
    rank:Int
                         )

object BoostedEntity {

    val RIOT_API_KEY = System.getenv("RIOT_API_KEY")

    val log = LoggerFactory.getLogger(BoostedEntity.getClass)

    def apply(from: BoostedSummonersChrolesToWR):BoostedEntity = {

        val summonerName = RedisStore.getSummonerNameById(from.region, from.summonerId).getOrElse({
            //Did not find the summonerName in the map, get it from riot
            val region = Region.valueOf(from.region)
            val config = new ApiConfig() ;
            config.setKey(RIOT_API_KEY)
            val riotApi = new RiotApi(config)
            val summonerName = riotApi.getSummonerById(region, from.summonerId).getName()
            log.debug(s"Retrieved from riot, summoner: ${from.summonerId} -> $summonerName")
            RedisStore.addSummonerName(from.region, from.summonerId, summonerName)
            summonerName
        })
        val matches = from.matches.toArray.toList
        BoostedEntity(
            Champions.byId(from.championId),
            Role.byId(from.roleId).toString,
            from.summonerId,
            summonerName,
            from.region,
            Tier.UNRANKED.toString,
            from.gamesPlayed,
            from.winrate,
            matches,
            from.rank
        )
    }
}
