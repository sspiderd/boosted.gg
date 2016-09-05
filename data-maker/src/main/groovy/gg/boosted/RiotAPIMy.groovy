package gg.boosted

import groovy.json.JsonSlurper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by ilan on 8/30/16.
 */
class RiotAPIMy {

    static Logger log = LoggerFactory.getLogger(RiotAPIMy.class)

    static API_KEY = ""

    static def get(String url) {
        return new JsonSlurper().parseText(getAsJson(url));
    }

    static String getAsJson(String url) {
        def sleep = 1000;

        if (url.contains("?")) {
            url = "${url}&api_key=${API_KEY}"
        } else {
            url = "${url}?api_key=${API_KEY}"
        }

        def retrieved = false ;
        while (!retrieved) {
            try {
                String resp = new URL(url).text ;
                retrieved = true ;
                return resp ;
            } catch (Exception ex) {
                log.debug("${ex.getMessage()} -> Sleeping for ${sleep}")
                Thread.sleep(sleep) ;
                sleep *= 2;
            }
        }
    }

    static def getChallengers(region) {
        return get("https://euw.api.pvp.net/api/lol/${region}/v2.5/league/challenger?type=RANKED_SOLO_5x5")
    }

    static List<String> getChallengerIds(region) {
        return getChallengers(region)["entries"].collect { it["playerOrTeamId"]}
    }

    static def getMasters(region) {
        return get("https://euw.api.pvp.net/api/lol/${region}/v2.5/league/master?type=RANKED_SOLO_5x5")
    }

    static List<String> getMastersIds(region) {
        return getMasters(region)["entries"].collect { it["playerOrTeamId"]}
    }

    static def getMatch(matchId, region) {
        return get("https://euw.api.pvp.net/api/lol/${region}/v2.2/match/${matchId}?includeTimeline=false") ;
    }

    /**
     *
     * @param summonerId
     * @param region
     * @param since (unix timestamp millis)
     * @return
     */
    static def getMatchlistForSummoner(summonerId, region, since) {
        return get("https://euw.api.pvp.net/api/lol/${region}/v2.2/matchlist/by-summoner/${summonerId}" +
                "?rankedQueues=TEAM_BUILDER_DRAFT_RANKED_5x5,RANKED_SOLO_5x5" +
                "&beginTime=$since");
    }

}