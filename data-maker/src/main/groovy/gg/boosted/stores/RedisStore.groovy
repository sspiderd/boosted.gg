package gg.boosted.stores;

import gg.boosted.configuration.Configuration;
import groovy.transform.CompileStatic;
import redis.clients.jedis.Jedis;

/**
 * Created by ilan on 8/30/16.
 */
@CompileStatic
class RedisStore {

    private static Jedis jedis = new Jedis(Configuration.getString("redis.location"));

    private static Long matchesProcessedTTL = Configuration.getLong("window.size.minutes") * 60 ;

    private static String matchesProcessed = "matchesProcessed"

    private static String summonersInQueue = "summonersInQueue"

    private static String summonersProcessed = "summonersProcessed"
    
    private static String summonerAccounts = "summonerAccount"
    
    private static String summonerNames = "summonerNames"

    static void reset() {
        jedis.flushAll();
    }

    static void addMatchToProcessedMatches(String platform, String matchId) {
        jedis.set("$matchesProcessed:$platform:$matchId", "", "NX", "EX", matchesProcessedTTL) ;
    }

    static Long addSummonersToQueue(String platform, String... summonerIds) {
        return jedis.rpush("$summonersInQueue:$platform", summonerIds)
    }

    static String popSummonerFromQueue(String platform) {
        return jedis.lpop("$summonersInQueue:$platform")
    }

    static void addSummonersProcessed(String platform, String... summonerIds) {
        jedis.sadd("$summonersProcessed:$platform", summonerIds)
    }

    static boolean wasMatchProcessedAlready(String platform, String matchId) {
        return jedis.get("$matchesProcessed:$platform:$matchId") != null ;
    }

    static boolean wasSummonerProcessedAlready(String platform, String summonerId) {
        return jedis.smembers("$summonersProcessed:$platform").contains(summonerId)
    }

    static void addSummonerAccount(String platform, String summonerId, String accountId) {
        jedis.set("$summonerAccounts:$platform:$summonerId", "$accountId")
    }

    static String getSummonerAccount(String platform, String summonerId) {
        jedis.get("$summonerAccounts:$platform:$summonerId")
    }

    static void addSummonerName(String platform, String summonerId, String summonerName) {
        jedis.set("$summonerNames:$platform:$summonerId", summonerName)
    }

    static String getSummonerName(String platform, String summonerId) {
        return jedis.get("$summonerNames:$platform:$summonerId")
    }

}
