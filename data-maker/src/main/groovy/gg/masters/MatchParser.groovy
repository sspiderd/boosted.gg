package gg.masters

/**
 * Created by ilan on 8/11/16.
 */
class MatchParser {

    static List<SummonerGame> parseMatch(match) {
        List<SummonerGame> summonerGameList  = match["participants"].collect {
            SummonerGame summonerGame = new SummonerGame()
            summonerGame.championId = it["championId"]
            summonerGame.winner = it["stats"]["winner"]
            summonerGame.matchId = match["matchId"]

            //Ok, so these were easy, now the next 2 are a bit more difficuly
            String lane = it["timeline"]["lane"]
            if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
                summonerGame.role = lane
            } else if (lane == "BOTTOM") {
                //This is bot lane
                if (it["timeline"]["role"] == "DUO_CARRY") {
                    summonerGame.role = "ADC"
                } else {
                    summonerGame.role = "SUPPORT"
                }
            } else throw new RuntimeException("I Don't know this lane ${lane} !!")

            //And finally we extract The summonerId
            Integer participantId = it["participantId"]
            summonerGame.summonerId = match["participantIdentities"].find {it["participantId"] == participantId}["player"]["summonerId"]
            summonerGame
        }
    }

}
