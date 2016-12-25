package gg.boosted

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import gg.boosted.configuration.Configuration
import gg.boosted.riotapi.dtos.match.MatchDetail
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class FromFile {

    static Logger log = LoggerFactory.getLogger(FromFile.class)

    private static Jedis jedis = new Jedis(Configuration.getString("redis.location"));

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

            putInRedis(md)

            List<SummonerMatch> matchDetails = MatchParser.parseMatch(md)
            log.debug("Sending match details for match ${md.matchId}")
            matchDetails.each {
                KafkaSummonerMatchProducer.send(it)
                sleep 5
            }
        }
    }

    static void putInRedis(MatchDetail md) {
        jedis.setex("fullMatch:${md.region}:${md.matchId}", 6000, new ObjectMapper().writeValueAsString(md))
    }

}
