package gg.boosted

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import gg.boosted.riotapi.dtos.match.MatchDetail
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class FromFile {

    static Logger log = LoggerFactory.getLogger(FromFile.class)

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

    static void main(String[] args) {
        String matchesText = FromFile.getClass().getResource( '/matches1.json' ).openStream().text
        ObjectMapper om = new ObjectMapper()
        Iterator it = om.readValue(matchesText, JsonNode.class).get("matches").iterator()
        while (it.hasNext()) {
        //new JsonSlurper().parseText(matchesText)['matches'].each { match ->
            MatchDetail md = om.readValue(it.next().toString(), MatchDetail.class)
            List<SummonerMatch> matchDetails = MatchParser.parseMatch(md)
            log.debug("Sending match details for match ${md.matchId}")
            matchDetails.each {
                KafkaSummonerMatchProducer.send(it)
                sleep 10
            }
        }
    }

}
