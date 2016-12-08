package gg.boosted.dal

import gg.boosted.analyzers.MostBoostedSummoners
import gg.boosted.maps.{Champions, SummonerIdToLoLScore, SummonerIdToName}
import gg.boosted.Role
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

    def apply(from: MostBoostedSummoners):BoostedEntity = {

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
