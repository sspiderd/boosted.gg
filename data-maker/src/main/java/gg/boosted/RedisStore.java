package gg.boosted;

import redis.clients.jedis.Jedis;

/**
 * Created by ilan on 8/30/16.
 */
public class RedisStore {

    private static Jedis jedis = new Jedis("10.0.0.3");

    private static String summonersInQueue = "summonersInQueue" ;

    private static String summonersProcessed = "summonersProcessed" ;

    private static String matchesProcessed = "matchesProcessed" ;

    public static void reset(String region) {
        jedis.del(summonersInQueue + "-" + region) ;
        jedis.del(summonersProcessed + "-" + region) ;
        jedis.del(matchesProcessed + "-" + region) ;
    }

    public static void addMatchesToProcessedMatches(String region, String... matchIds) {
        jedis.sadd(matchesProcessed + "-" + region, matchIds) ;
    }

    public static Long addSummonersToQueue(String region, String... summonerIds) {
        return jedis.rpush(summonersInQueue + "-" + region, summonerIds) ;
    }

    public static String popSummonerFromQueue(String region) {
        return jedis.lpop(summonersInQueue + "-" + region) ;
    }

    public static Long numberOfSummonersInQueue() {
        return jedis.llen(summonersInQueue) ;
    }

    public static void addSummonersProcessed(String region, String... summonerIds) {
        jedis.sadd(summonersProcessed + "-" + region, summonerIds) ;
    }

    public static boolean wasMatchProcessedAlready(String region, String matchId) {
        return jedis.smembers(matchesProcessed + "-" + region).contains(matchId) ;
    }

    public static boolean wasSummonerProcessedAlready(String region, String summonerId) {
        return jedis.smembers(summonersProcessed + "-" + region).contains(summonerId) ;
    }

}
