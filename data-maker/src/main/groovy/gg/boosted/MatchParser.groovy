package gg.boosted

import gg.boosted.riotapi.dtos.match.MatchDetail
import groovy.transform.CompileStatic

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class MatchParser {

    static List<SummonerMatch> parseMatch(MatchDetail match) {
        if (!match) return new LinkedList<SummonerMatch>()
        match.participants.collect {
            SummonerMatch summonerMatch = new SummonerMatch()
            summonerMatch.championId = it.championId
            summonerMatch.winner = it.stats.winner
            summonerMatch.matchId = match.matchId
            summonerMatch.region = match.region.toString()
            summonerMatch.date = match.matchCreation

            //Ok, so these were easy, now the next 2 are a bit more difficult
            String lane = it.timeline.lane
            String role
            if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
                role = lane
            } else if (lane == "BOTTOM") {
                //This is bot lane
                if (it.timeline.role ==  "DUO_CARRY") {
                    role = "BOTTOM"
                } else {
                    role = "SUPPORT"
                }
            } else throw new RuntimeException("I Don't know this lane ${lane} !!")

            summonerMatch.roleId = Role.valueOf(role).roleId

            //And finally we extract The summonerId
            Integer participantId = it.participantId
            summonerMatch.summonerId = match.participantIdentities.find {it.participantId == participantId}.player.summonerId
            summonerMatch
        }
    }

}
