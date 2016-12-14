package gg.boosted;

import gg.boosted.configuration.Configuration;
import groovy.transform.CompileStatic;
import redis.clients.jedis.Jedis;

/**
 * Created by ilan on 8/30/16.
 */
@CompileStatic
public class RedisStore {

    private static Jedis jedis = new Jedis(Configuration.getString("redis.location"));

    private static Long TTL = Configuration.getLong("window.size.seconds") ;

    private static String summonersInQueue = "summonersInQueue" ;

    private static String summonersProcessed = "summonersProcessed" ;

    public static void reset() {
        jedis.flushAll();
    }

    public static void addMatchToProcessedMatches(String region, String matchId) {
        jedis.set("matchesProcessed:" + region + ":" + matchId, "", "NX", "EX", TTL) ;
    }

    public static Long addSummonersToQueue(String region, String... summonerIds) {
        return jedis.rpush(summonersInQueue + ":" + region, summonerIds) ;
    }

    public static String popSummonerFromQueue(String region) {
        return jedis.lpop(summonersInQueue + ":" + region) ;
    }

    public static void addSummonersProcessed(String region, String... summonerIds) {
        jedis.sadd(summonersProcessed + ":" + region, summonerIds) ;
    }

    public static boolean wasMatchProcessedAlready(String region, String matchId) {
        return jedis.get("matchesProcessed:" + region + ":" + matchId) != null ;
    }

    public static boolean wasSummonerProcessedAlready(String region, String summonerId) {
        return jedis.smembers(summonersProcessed + ":" + region).contains(summonerId) ;
    }

}
