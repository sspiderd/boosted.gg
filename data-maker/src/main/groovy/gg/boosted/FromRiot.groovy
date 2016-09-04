package gg.boosted

import groovy.transform.CompileStatic
import net.rithms.riot.api.ApiConfig
import net.rithms.riot.api.RiotApi
import net.rithms.riot.api.endpoints.match.dto.MatchDetail
import net.rithms.riot.constant.QueueType
import net.rithms.riot.constant.Region
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneId

/**
 * Created by ilan on 8/30/16.
 */
@CompileStatic
class FromRiot {

    static Logger log = LoggerFactory.getLogger(FromRiot)

    static RiotApi riotApi

    static Region region

    public static void main(String[] args) {
        ApiConfig config = new ApiConfig()
        config.setKey(RiotAPIMy.API_KEY)
        riotApi = new RiotApi(config)

        extract(Region.EUW)
    }


    static def extract(Region region) {

        this.region = region ;

        //Forget that summoners and matches were ever processed
        //Remove all summoners and matches from redis
        RedisStore.reset() ;

        //Create an empty set of summonerIds.. This is the queue to which we add new summoners that we find
        //Get an initial seed of summoners
        //List<String> summonerQueue = getInitialSummonerSeed(region)
        //RedisStore.addSummonersToQueue(summonerQueue)
        List<String> summonerQueue = getInitialSummonerSeed(region)
        RedisStore.addSummonersToQueue(summonerQueue as String[])

        //get the time at which we want to look matches from then on
        long gamesPlayedSince = getDateToLookForwardFrom()

        //While the queue is not empty
        String summonerId = null
        while ((summonerId = RedisStore.popSummonerFromQueue()) != null) {
            //Get the next summoner (it's in summonerId)

            //Check that we haven't seen him yet
            if (RedisStore.wasSummonerProcessedAlready(summonerId)) {
                log.debug("Summoner ${summonerId} was already processed...")
                continue
            }

            long time = System.currentTimeMillis()

            log.debug("Processing summoner ${summonerId}")

            //Get his matches since $gamesPlayedSince
            List<Long> matchIds = getSummonerMatchIds(summonerId, gamesPlayedSince)

            //For each match:
            matchIds.each {

                //Check that we haven't seen this match yet
                if (!RedisStore.wasMatchProcessedAlready(it.toString())) {

                    log.debug("Processing match ${it}")

                    //Get the match itself
                    MatchDetail match = riotApi.getMatch(region, it, false)
                    //def match = RiotAPIMy.getMatch(it, region.toLowerCase())

                    //create "SummonerMatch" items for each summoner in the match
                    List<SummonerMatch> summonerMatchList = MatchParser.parseMatch(match)

                    //Send them all to the broker
                    summonerMatchList.each {
                        KafkaSummonerGameProducer.send(it)
                    }

                    //Add the match to "seen matches"
                    RedisStore.addMatchesToProcessedMatches(it.toString())

                    //Add all the summoners to the summoner queue
                    summonerMatchList.each {RedisStore.addSummonersToQueue(it.summonerId.toString())}
                } else {
                    log.debug("Match ${it} was already processed...")
                }
            }

            //The summoner is now processed. Add her to the queue
            RedisStore.addSummonersProcessed(summonerId)

            log.debug("Time taken to process summoner = ${(System.currentTimeMillis() - time) / 1000}S")
        }
    }

    static List<Long> getSummonerMatchIds(String summonerId, long since) {
        riotApi.getMatchList(region, summonerId as long, null, null, null, since, -1L, -1, -1).getMatches().collect {it.matchId}
        //riotApi.getMatchList(summonerId.toLong(), new Date(since)).collect {it.getID()}
        //return RiotAPIMy.getMatchlistForSummoner(summonerId, region, since)["matches"].collect {it["matchId"].toString()}
    }

    static List<String> getInitialSummonerSeed(Region region) {
        //List<String> seed = RiotAPIMy.getChallengerIds(region)
        List<String> seed = []
        riotApi.getChallengerLeague(region, QueueType.RANKED_SOLO_5x5).entries.each {
            seed += it.getPlayerOrTeamId()
        }
        riotApi.getMasterLeague(region, QueueType.RANKED_SOLO_5x5).entries.each {
            seed += it.getPlayerOrTeamId()
        }

        //seed += RiotAPIMy.getMastersIds(region)

        return seed
    }

    static long getDateToLookForwardFrom() {
        //Two weeks ago
        LocalDateTime twoWeeksAgo = (LocalDateTime.now() - Period.ofWeeks(2))
        ZoneId zoneId = ZoneId.of("UTC")
        long epoch = twoWeeksAgo.atZone(zoneId).toEpochSecond() * 1000
        return epoch
    }

}
