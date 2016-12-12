package gg.boosted.dal

import gg.boosted.Role
import gg.boosted.analyzers.BoostedSummoner
import gg.boosted.maps.Champions
import gg.boosted.posos.LoLScore
import gg.boosted.riotapi.Region
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 9/1/16.
  */
case class BoostedEntity (
    champion:String,
    role:String,
    summonerId:Long,
    summonerName:String,
    region:Region,
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

    val log = LoggerFactory.getLogger(BoostedEntity.getClass)

    def toEntity(summoner: BoostedSummoner, name:String, lolScore:LoLScore): BoostedEntity = {
        val matches = summoner.matches.toArray.toList
        BoostedEntity(
            Champions.byId(summoner.championId),
            Role.byId(summoner.roleId).toString,
            summoner.summonerId.toLong,
            name,
            summoner.region,
            lolScore.tier,
            lolScore.division,
            lolScore.leaguePoints,
            lolScore.lolScore,
            summoner.gamesPlayed,
            summoner.winrate,
            matches,
            summoner.rank
        )
    }

}
