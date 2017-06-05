package gg.boosted

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import gg.boosted.configuration.Configuration

import groovy.json.JsonOutput
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
 * Created by ilan on 8/11/16.
 */
//@CompileStatic
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
        int counter = 0
        int matchCounter = 0
        (1..10).each {
            log.debug("Reading file ${it}")
            //String matchesJson = new URL("https://s3-us-west-1.amazonaws.com/riot-api/seed_data/matches${it}.json").getText()
            String matchesJson = FromFile.getClass().getResource("/matches${it}.json").openStream().text
            ObjectMapper om = new ObjectMapper()
            Iterator iter = om.readValue(matchesJson, JsonNode.class).get("matches").iterator()
            while (iter.hasNext()) {
                matchCounter++
                //new JsonSlurper().parseText(matchesText)['matches'].each { match ->
                MatchDetail md = om.readValue(iter.next().toString(), MatchDetail.class)

                putInRedis(md)

                List<SummonerMatch> matchDetails = MatchParser.parseMatch(md)
                log.debug("Sending match details for match ${md.matchId}")
                matchDetails.each {
                    counter++
                    KafkaSummonerMatchProducer.send(it)
                    sleep 1
                }
            }
        }

        println ("Sent total ${counter} summoner matches for ${matchCounter} matches")
    }

    static void putInRedis(MatchDetail md) {
        jedis.setex("fullMatch:${md.region}:${md.matchId}", 6000, new ObjectMapper().writeValueAsString(md))
    }

}
