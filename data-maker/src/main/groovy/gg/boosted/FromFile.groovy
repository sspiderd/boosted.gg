package gg.boosted

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Created by ilan on 8/11/16.
 */
class FromFile {

    /* Returns a list maps, each containing:
     *
     * MatchId
     * SummonerId
     * Champion
     * Role
     * Win/Loss
    */
    static List<SummonerMatch> parseMatch(match) {
        println JsonOutput.prettyPrint(JsonOutput.toJson(match))
    }

    public static void main(String[] args) {
        String matchesText = this.getClass().getResource( '/matches1.json' ).openStream().text
        new JsonSlurper().parseText(matchesText)['matches'].each { match ->
            List<SummonerMatch> summonerGameList = MatchParser.parseMatch(match) ;
            summonerGameList.each {
                println it
                KafkaSummonerGameProducer.send(it)
            }
            sleep 500
        }
    }

}
