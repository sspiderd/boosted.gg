package gg.boosted

/**
 * Created by ilan on 8/11/16.
 */
class MatchParser {

    static List<SummonerMatch> parseMatch(match) {
        List<SummonerMatch> summonerMatchList  = match["participants"].collect {
            SummonerMatch summonerMatch = new SummonerMatch()
            summonerMatch.championId = it["championId"]
            summonerMatch.winner = it["stats"]["winner"]
            summonerMatch.matchId = match["matchId"]
            summonerMatch.region = match["region"]
            summonerMatch.date = match["matchCreation"]
            summonerMatch.tier = it["highestAchievedSeasonTier"]

            //Ok, so these were easy, now the next 2 are a bit more difficuly
            String lane = it["timeline"]["lane"]
            if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
                summonerMatch.role = lane
            } else if (lane == "BOTTOM") {
                //This is bot lane
                if (it["timeline"]["role"] == "DUO_CARRY") {
                    summonerMatch.role = "BOTTOM"
                } else {
                    summonerMatch.role = "SUPPORT"
                }
            } else throw new RuntimeException("I Don't know this lane ${lane} !!")

            //And finally we extract The summonerId
            Integer participantId = it["participantId"]
            summonerMatch.summonerId = match["participantIdentities"].find {it["participantId"] == participantId}["player"]["summonerId"]
            summonerMatch
        }
        return summonerMatchList
    }

}
