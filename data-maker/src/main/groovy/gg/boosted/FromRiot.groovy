package gg.boosted

import com.robrua.orianna.api.core.RiotAPI
import com.robrua.orianna.type.api.LoadPolicy
import com.robrua.orianna.type.core.common.QueueType
import com.robrua.orianna.type.core.common.Region
import com.robrua.orianna.type.core.match.Match
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.LocalTime
import java.time.Period
import java.time.ZoneId

/**
 * Created by ilan on 8/30/16.
 */
class FromRiot {

    static Logger log = LoggerFactory.getLogger(FromRiot)

    public static void main(String[] args) {
        extract("EUW")
    }


    @CompileStatic
    static def extract(String region) {
        RiotAPI.setAPIKey(RiotAPIMy.API_KEY)
        RiotAPI.setRegion(Region.valueOf(region))
        RiotAPI.setLoadPolicy(LoadPolicy.LAZY)

        //Forget that summoners and matches were ever processed
        //Remove all summoners and matches from redis
        //RedisStore.reset() ;

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
                    Match match = RiotAPI.getMatch(it, false)
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

            log.debug("Time taken to process summoner = ${(System.currentTimeMillis() - time) / 1000}S")
        }
    }

    @CompileStatic
    static List<Long> getSummonerMatchIds(String summonerId, long since) {
        RiotAPI.getMatchList(summonerId.toLong(), new Date(since)).collect {it.getID()}
        //return RiotAPIMy.getMatchlistForSummoner(summonerId, region, since)["matches"].collect {it["matchId"].toString()}
    }

    @CompileStatic
    static List<String> getInitialSummonerSeed(String region) {
        //List<String> seed = RiotAPIMy.getChallengerIds(region)
        List<String> seed = []
        RiotAPI.getChallenger(QueueType.RANKED_SOLO_5x5).entries.each {
            seed += it.getID()
        }
        RiotAPI.getMaster(QueueType.RANKED_SOLO_5x5).entries.each {
            seed += it.getID()
        }


        //seed += RiotAPIMy.getMastersIds(region)

        return seed
    }

    @CompileStatic
    static long getDateToLookForwardFrom() {
        //Two weeks ago
        LocalDateTime twoWeeksAgo = (LocalDateTime.now() - Period.ofWeeks(2))
        ZoneId zoneId = ZoneId.of("UTC")
        long epoch = twoWeeksAgo.atZone(zoneId).toEpochSecond() * 1000
        return epoch
    }


    static def extract1(region) {

        //Delete the initial summoner list from redis
        RedisStore.reset() ;

        //Get the initial seed of summoners for this region (these are the challengers)
        def summonersToBeProcessed = RiotAPIMy.getChallengerIds(region)[0..3]

        summonersToBeProcessed += RiotAPIMy.getMastersIds(region)[0..2]

        //Initially add the seed to the set
        def numberOfSummonersToBeProcessed = RedisStore.addSummonersToQueue(summonersToBeProcessed as String[]);

        //Calculate what time it was 2 weeks ago, since this is the "begin time" for us
        def twoWeeksAgo = (LocalTime.now() - Period.ofWeeks(2)).getLong()

        while (numberOfSummonersToBeProcessed > 0) {
            String summonerId = RedisStore.popSummonerFromQueue() ;

            processSummoner(summonerId, region, twoWeeksAgo)

            if (!RedisStore.wasSummonerProcessedAlready(summonerId)) {

                //We did not see that summoner yet, process her matches
                String[] newSummonersToProcess = processMatchesForSummoner(summonerId, region) ;

                RedisStore.addSummonersToQueue(newSummonersToProcess)

                RedisStore.addSummonersProcessed(summonerId)
            } else {
                println "Summoner ${summonerId} was already processed..."
            }
            numberOfSummonersToBeProcessed = RedisStore.numberOfSummonersInQueue() ;
        }
    }

    static String[] processSummoner(String summonerId, String region, since) {

        //Check whether we processed the guy already
        if (RedisStore.wasSummonerProcessedAlready(summonerId)) {
            return
        }

        //Now get his matches for the past 2 weeks
        def summonerMatches = RiotAPIMy.getMatchlistForSummoner(summonerId, region, since)

        //Extract the matchIds
        def summonerMatchesIds = summonerMatches.collect { it["matchId"] }

        println ("Processing matches: ${summonerMatchesIds} for summoner: ${summonerId}...")

        Set<String> newSummonersToProcess = []

        summonerMatchesIds.each { newSummonersToProcess += processMatch (it, region) }

        return newSummonersToProcess.toArray(new String[0])

    }

    /**
     * And by process match i mean:
     *
     * Check that it wasn't processed yet
     * Convert it to "SummonerMatch" object
     * Send it through kafka (or whatever messaging implementation i'll be using)
     * And tell Redis that it is now processed
     *
     * @param matchId
     */
    static def processMatch(String matchId, String region) {
        if (RedisStore.wasMatchProcessedAlready(matchId)) {
            return
        }

        def match = RiotAPIMy.getMatch(matchId, region)

        List<SummonerMatch> summonerMatches = MatchParser.parseMatch(match)

        Set<String> summonerIdsInMatch = []

        summonerMatches.each {
            summonerIdsInMatch += it.summonerId
            KafkaSummonerGameProducer.send(it)
        }

        return summonerIdsInMatch
    }

    static List<Integer> getSummonerIdsForMatch(match) {
        return match["participantIdentities"].collect { it["player"] }.collect { it["summonerId"] }
    }

    static def processMatchesForSummoner(summonerId, region) {

        def fullMatchList = RiotAPIMy.getMatchlistForSummoner(summonerId, region);

        def matchList = fullMatchList["matches"].collect() { it["matchId"]} ;

        println ("Processing matches: ${matchList} for summoner: ${summonerId}...")

        matchList.each { matchId ->
            //Process only matches we haven't processed before
            def matchExists = RedisStore.wasMatchProcessedAlready(matchId as String) ;

            if (!matchExists) {
                def match = RiotAPIMy.getMatch(matchId, region)

                RedisStore.addMatchesToProcessedMatches(matchId as String)
            } else {
                println "Match ${matchId} was already processed..."
            }
        }
    }
}
