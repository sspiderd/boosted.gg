package gg.boosted

import com.robrua.orianna.type.core.common.Lane
import com.robrua.orianna.type.core.match.Match
import groovy.transform.CompileStatic

/**
 * Created by ilan on 8/11/16.
 */
class MatchParser {

    @CompileStatic
    static List<SummonerMatch> parseMatch(Match match) {
        match.participants.collect {
            SummonerMatch summonerMatch = new SummonerMatch()
            summonerMatch.championId = it.championID
            summonerMatch.winner = it.stats.winner
            summonerMatch.matchId = match.ID
            summonerMatch.region = match.region.toString()
            summonerMatch.date = match.creation.getTime()
            summonerMatch.tier = -1

            //Ok, so these were easy, now the next 2 are a bit more difficult
            Lane lane = it.timeline.lane
            String role = null
            if (lane == Lane.TOP || lane == Lane.MIDDLE || lane == Lane.JUNGLE) {
                role = lane.toString()
            } else if (lane == Lane.BOTTOM) {
                //This is bot lane

                if (it.timeline.role == com.robrua.orianna.type.core.common.Role.DUO_CARRY) {
                    role = "BOTTOM"
                } else {
                    role = "SUPPORT"
                }
            } else throw new RuntimeException("I Don't know this lane ${lane} !!")

            summonerMatch.roleId = Role.valueOf(role).roleId

            //And finally we extract The summonerId
            Integer participantId = it.participantID
            summonerMatch.summonerId = match.dto.participantIdentities.find {it.participantId == participantId}.player.summonerId
            summonerMatch
        }
    }

    static List<SummonerMatch> parseMatch(match) {
        List<SummonerMatch> summonerMatchList  = match["participants"].collect {
            SummonerMatch summonerMatch = new SummonerMatch()
            summonerMatch.championId = it["championId"]
            summonerMatch.winner = it["stats"]["winner"]
            summonerMatch.matchId = match["matchId"]
            summonerMatch.region = match["region"]
            summonerMatch.date = match["matchCreation"]
            summonerMatch.tier = Tier.valueOf(it["highestAchievedSeasonTier"]).tierId

            //Ok, so these were easy, now the next 2 are a bit more difficuly
            String lane = it["timeline"]["lane"]
            String role = null
            if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
                role = lane
            } else if (lane == "BOTTOM") {
                //This is bot lane
                if (it["timeline"]["role"] == "DUO_CARRY") {
                    role = "BOTTOM"
                } else {
                    role = "SUPPORT"
                }
            } else throw new RuntimeException("I Don't know this lane ${lane} !!")

            summonerMatch.roleId = Role.valueOf(role).roleId

            //And finally we extract The summonerId
            Integer participantId = it["participantId"]
            summonerMatch.summonerId = match["participantIdentities"].find {it["participantId"] == participantId}["player"]["summonerId"]
            summonerMatch
        }
        return summonerMatchList
    }

}
