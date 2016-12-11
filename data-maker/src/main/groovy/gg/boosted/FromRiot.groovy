package gg.boosted

import groovy.json.JsonSlurper
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
class FromRiot {

    static Logger log = LoggerFactory.getLogger(FromRiot)

    static RiotApi riotApi

    static gg.boosted.RiotApi riotApi1 ;

    static Region region

    public static void main(String[] args) {
        ApiConfig config = new ApiConfig()
        config.setKey(System.getenv("RIOT_API_KEY"))
        riotApi = new RiotApi(config)

        riotApi1 = new gg.boosted.RiotApi(gg.boosted.Region.EUW) ;

        extract(Region.EUW)
    }


    static def extract(Region region) {

        this.region = region ;

        //Forget that summoners and matches were ever processed
        //Remove all summoners and matches from redis
        RedisStore.reset(region.toString()) ;

        //Create an empty set of summonerIds.. This is the queue to which we add new summoners that we find
        //Get an initial seed of summoners
        //List<String> summonerQueue = getInitialSummonerSeed(region)
        //RedisStore.addSummonersToQueue(summonerQueue)
        List<String> summonerQueue = getInitialSummonerSeed(region)
        RedisStore.addSummonersToQueue(region.toString(), summonerQueue as String[])

        //get the time at which we want to look matches from then on
        long gamesPlayedSince = getDateToLookForwardFrom()

        //While the queue is not empty
        String summonerId = null
        while ((summonerId = RedisStore.popSummonerFromQueue(region.toString())) != null) {
            //Get the next summoner (it's in summonerId)

            //Check that we haven't seen him yet
            if (RedisStore.wasSummonerProcessedAlready(region.toString(), summonerId)) {
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
                if (!RedisStore.wasMatchProcessedAlready(region.toString(), it.toString())) {

                    log.debug("Processing match ${it}")

                    //Get the match itself
                    MatchDetail match = riotApi.getMatch(region, it, false)
                    //def match = RiotAPIMy.getMatch(it, region.toLowerCase())

                    //create "SummonerMatch" items for each summoner in the match
                    List<SummonerMatch> summonerMatchList = MatchParser.parseMatch(match)

                    //Send them all to the broker
                    summonerMatchList.each {
                        KafkaSummonerMatchProducer.send(it)
                    }

                    //Add the match to "seen matches"
                    RedisStore.addMatchesToProcessedMatches(region.toString(), it.toString())

                    //Add all the summoners to the summoner queue
                    summonerMatchList.each {RedisStore.addSummonersToQueue(region.toString(), it.summonerId.toString())}
                } else {
                    log.debug("Match ${it} was already processed...")
                }
            }

            //The summoner is now processed. Add her to the queue
            RedisStore.addSummonersProcessed(region.toString(), summonerId)

            log.debug("Time taken to process summoner = ${(System.currentTimeMillis() - time) / 1000}S")
        }
    }

    static List<Long> getSummonerMatchIds(String summonerId, long since) {
        //riotApi.getMatchList(region, summonerId as long, null, QueueType.RANKED_SOLO_5x5.toString(), null, since, -1L, -1, -1).getMatches().collect {it.matchId}
        //riotApi.getMatchList(summonerId.toLong(), new Date(since)).collect {it.getID()}
        //return RiotAPIMy.getMatchlistForSummoner(summonerId, region, since)["matches"].collect {it["matchId"].toString()}
        riotApi1.getMatchList(summonerId as long, since).collect {it.matchId}
    }

    static List<String> getInitialSummonerSeed(Region region) {
        List<String> seed = []

        //At the beginning of the season there are no challengers and masters, so the following
        //API calls will return null
        def entries = riotApi.getChallengerLeague(region, QueueType.RANKED_SOLO_5x5).entries
        if (entries != null) entries.each {
            seed += it.getPlayerOrTeamId()
        }
        entries = riotApi.getMasterLeague(region, QueueType.RANKED_SOLO_5x5).entries
        if (entries != null) entries.each {
            seed += it.getPlayerOrTeamId()
        }

        //If there are no challengers or masters (since it's the start of the season)
        //Try to get a seed from some random game
        if (seed.size() == 0) {
            List<String> summonerNames = []
            new JsonSlurper().parseText(riotApi1.getFeaturedGames())["gameList"].each {match ->
                match["participants"].each {participant -> summonerNames += participant["summonerName"]} ;
            }
            Map<String, String> namesToIds = riotApi1.getSummonerIdsByNames(summonerNames.toArray(new String[0])) ;
            seed = namesToIds.values() ;
        }

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
