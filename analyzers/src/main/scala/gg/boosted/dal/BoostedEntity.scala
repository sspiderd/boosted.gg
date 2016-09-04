package gg.boosted.dal

import gg.boosted.analyzers.BoostedSummonersChrolesToWR
import net.rithms.riot.api.{ApiConfig, RiotApi}
import net.rithms.riot.constant.Region
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 9/1/16.
  */
case class BoostedEntity (
    championId:Int,
    roleId:Int,
    summonerId:Long,
    summonerName:String,
    region:String,
    tier:Int,
    gamesPlayed:Long,
    winrate:Double,
    matches:List[Long]
                         )

object BoostedEntity {

    val RIOT_API_KEY = "RGAPI-30F1B978-7AC1-4B68-B016-A1975D1DDD94"

    val log = LoggerFactory.getLogger(BoostedEntity.getClass)

    def apply(from: BoostedSummonersChrolesToWR):BoostedEntity = {

        val summonerName = RedisStore.getSummonerNameById(from.summonerId).getOrElse({
            //Did not find the summonerName in the map, get it from riot
            val region = Region.valueOf(from.region)
            val config = new ApiConfig() ;
            config.setKey(RIOT_API_KEY)
            val riotApi = new RiotApi(config)
            val summonerName = riotApi.getSummonerById(region, from.summonerId).getName()
            log.debug(s"Retrieved from riot, summoner: ${from.summonerId} -> $summonerName")
            RedisStore.addSummonerName(from.summonerId, summonerName)
            summonerName
        })
        val matches = from.matches.toArray.toList
        BoostedEntity(
            from.championId,
            from.roleId,
            from.summonerId,
            summonerName,
            from.region,
            -1,
            from.gamesPlayed,
            from.winrate,
            matches
        )
    }
}
