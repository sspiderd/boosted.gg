package gg.boosted.dal

import gg.boosted.{Champions, Role, Tier}
import gg.boosted.analyzers.BoostedSummonersChrolesToWR
import gg.boosted.maps.{SummonerIdToLoLScore, SummonerIdToName}
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
    division:String,
    leaguePoints:Int,
    lolScore:Int,
    gamesPlayed:Long,
    winrate:Double,
    matches:List[Long],
    rank:Int
                         )

object BoostedEntity {

    val RIOT_API_KEY = System.getenv("RIOT_API_KEY")

    val log = LoggerFactory.getLogger(BoostedEntity.getClass)

    def apply(from: BoostedSummonersChrolesToWR):BoostedEntity = {

        val summonerName = SummonerIdToName(from.region, from.summonerId)
        val lolScore = SummonerIdToLoLScore(from.region, from.summonerId)
        val matches = from.matches.toArray.toList
        BoostedEntity(
            Champions.byId(from.championId),
            Role.byId(from.roleId).toString,
            from.summonerId,
            summonerName,
            from.region,
            lolScore.tier,
            lolScore.division,
            lolScore.leaguePoints,
            lolScore.lolScore,
            from.gamesPlayed,
            from.winrate,
            matches,
            from.rank
        )
    }
}
