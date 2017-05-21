package gg.boosted

import gg.boosted.riotapi.dtos.match.MatchDetail
import groovy.transform.CompileStatic

import java.util.regex.Matcher

/**
 * Created by ilan on 8/11/16.
 */
class MatchParser {

    static List<SummonerMatch> parseMatch(MatchDetail match) {

        if (!match) return new LinkedList<SummonerMatch>()
        Matcher patch = (match.matchVersion =~ /(\d+)\.(\d+).*/)
        match.participants.collect {
            SummonerMatch summonerMatch = new SummonerMatch()
            summonerMatch.championId = it.championId
            summonerMatch.winner = it.stats.winner ? 1 : 0
            summonerMatch.matchId = match.matchId
            summonerMatch.region = match.region.toString()
            summonerMatch.creationDate = match.matchCreation

            summonerMatch.patchMajorVersion = patch[0][1].toInteger()
            summonerMatch.patchMinorVersion = patch[0][2].toInteger()

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
