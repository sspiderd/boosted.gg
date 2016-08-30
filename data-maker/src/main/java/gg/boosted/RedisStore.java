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

    public static void reset() {
        jedis.del(summonersInQueue) ;
        jedis.del(summonersProcessed) ;
        jedis.del(matchesProcessed) ;
    }

    public static void addMatchesToProcessedMatches(String... matchIds) {
        jedis.sadd(matchesProcessed, matchIds) ;
    }

    public static Long addSummonersToQueue(String... summonerIds) {
        return jedis.lpush(summonersInQueue, summonerIds) ;
    }

    public static String popSummonerFromQueue() {
        return jedis.lpop(summonersInQueue) ;
    }

    public static Long numberOfSummonersInQueue() {
        return jedis.llen(summonersInQueue) ;
    }

    public static void addSummonersProcessed(String... summonerIds) {
        jedis.sadd(summonersProcessed, summonerIds) ;
    }

    public static boolean wasMatchProcessedAlready(String matchId) {
        return jedis.smembers(matchesProcessed).contains(matchId) ;
    }

    public static boolean wasSummonerProcessedAlready(String summonerId) {
        return jedis.smembers(summonersProcessed).contains(summonerId) ;
    }

}
