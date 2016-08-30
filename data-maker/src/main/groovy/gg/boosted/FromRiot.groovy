package gg.boosted

import java.time.LocalTime
import java.time.Period
import java.time.temporal.TemporalAmount

/**
 * Created by ilan on 8/30/16.
 */
class FromRiot {

    public static void main(String[] args) {
        extract("EUW")
    }


    static def extract1(region) {

        //Forget that summoners and matches were ever processed
        //Remove all summoners and matches from redis

        //Create an empty set of summonerIds.. This is the queue to which we add new summoners that we find
        Set<Long> summonerQueue = []

        //Get an initial seed of summoners
        //summonerQueue += getInitialSummonersSeed()

        //get the time at which we want to look matches from then on
        //long gamesPlayedSince = getDateToLookForwardFrom()

        //While the queue is not empty (i'll continue tomorrow)



    }


    static def extract(region) {

        //Delete the initial summoner list from redis
        RedisStore.reset() ;

        //Get the initial seed of summoners for this region (these are the challengers)
        def summonersToBeProcessed = RiotAPI.getChallengerIds(region)[0..3]

        summonersToBeProcessed += RiotAPI.getMastersIds(region)[0..2]

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
        def summonerMatches = RiotAPI.getMatchlistForSummoner(summonerId, region, since)

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

        def match = RiotAPI.getMatch(matchId, region)

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

        def fullMatchList = RiotAPI.getMatchlistForSummoner(summonerId, region);

        def matchList = fullMatchList["matches"].collect() { it["matchId"]} ;

        println ("Processing matches: ${matchList} for summoner: ${summonerId}...")

        matchList.each { matchId ->
            //Process only matches we haven't processed before
            def matchExists = RedisStore.wasMatchProcessedAlready(matchId as String) ;

            if (!matchExists) {
                def match = RiotAPI.getMatch(matchId, region)

                RedisStore.addMatchesToProcessedMatches(matchId as String)
            } else {
                println "Match ${matchId} was already processed..."
            }
        }
    }
}
