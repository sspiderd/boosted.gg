package gg.boosted

import gg.boosted.riotapi.Region
import gg.boosted.riotapi.RiotApi
import gg.boosted.riotapi.dtos.match.MatchDetail
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
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

    static void main(String[] args) {

        riotApi = new RiotApi(Region.EUW) ;

        extract(Region.EUW)
    }


    static def extract(Region region) {

        this.region = region

        //Forget that summoners and matches were ever processed
        //Remove all summoners and matches from redis
        RedisStore.reset(region.toString()) ;

        //Create an empty set of summonerIds.. This is the queue to which we add new summoners that we find
        //Get an initial seed of summoners
        //List<String> summonerQueue = getInitialSummonerSeed(region)
        //RedisStore.addSummonersToQueue(summonerQueue)
        List<Long> summonerQueue = getInitialSummonerSeed(region)
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
                    MatchDetail match = riotApi.getMatch(it, false)
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
        riotApi.getMatchList(summonerId as long, since).collect {it.matchId}
    }

    static List<Long> getInitialSummonerSeed(Region region) {
        List<Long> seed = []

        //At the beginning of the season there are no challengers and masters, so the following
        //API calls will return null
        seed.addAll(riotApi.getChallengersIds())
        seed.addAll(riotApi.getMastersIds())

        log.debug("Found total {} challengers + masters", seed.size())

        //If there are no challengers or masters (since it's the start of the season)
        //Try to get a seed from some random game
        if (seed.size() == 0) {
            log.info("There are currently no challengers or masters. getting seed from featured games...")
            List<String> summonerNames = []
            new JsonSlurper().parseText(riotApi.getFeaturedGames())["gameList"].each { match ->
                match["participants"].each {participant -> summonerNames += (String)participant["summonerName"]}
            }
            Map<String, Long> namesToIds = riotApi.getSummonerIdsByNames(summonerNames.toArray(new String[0]))
            seed.addAll(namesToIds.values())
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
