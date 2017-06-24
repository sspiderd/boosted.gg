package gg.boosted

import gg.boosted.riotapi.dtos.match.Match

import java.util.regex.Matcher

/**
 * Created by ilan on 8/11/16.
 */
class MatchParser {

    static List<SummonerMatch> parseMatch(Match match) {

        if (!match) return new LinkedList<SummonerMatch>()
        Matcher patch = (match.gameVersion =~ /(\d+)\.(\d+).*/)
        match.participants.collect {
            SummonerMatch summonerMatch = new SummonerMatch()
            summonerMatch.championId = it.championId
            summonerMatch.winner = it.stats.win ? 1 : 0
            summonerMatch.gameId = match.gameId
            summonerMatch.platformId = match.platformId.toString()
            summonerMatch.creationDate = match.gameCreation

            summonerMatch.patchMajorVersion = patch[0][1].toInteger()
            summonerMatch.patchMinorVersion = patch[0][2].toInteger()

            String role = normalizedRole(it.timeline.lane, it.timeline.role)

            summonerMatch.role = Role.valueOf(role)

            //And finally we extract The summonerId
            Integer participantId = it.participantId
            summonerMatch.summonerId = match.participantIdentities.find {it.participantId == participantId}.player.summonerId
            summonerMatch
        }
    }

    static String normalizedRole(String lane, String role) {
        String normalizedRole
        if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
            normalizedRole = lane
        } else if (lane == "BOTTOM") {
            //This is bot lane
            if (role ==  "DUO_CARRY") {
                normalizedRole = "BOTTOM"
            } else {
                normalizedRole = "SUPPORT"
            }
        } else throw new RuntimeException("I Don't know this lane ${lane} !!")
        return normalizedRole
    }

}
