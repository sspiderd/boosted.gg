package gg.boosted.riotapi.throttlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Random;

/**
 * This throttler is based on the "redlock" algorithm (https://redis.io/topics/distlock)
 * It is used to coordinate API calls between several processes using redis as a lockRes mechanism
 *
 * Created by ilan on 12/12/16.
 */
public class DistributedThrottler implements IThrottler{

    private static Logger log = LoggerFactory.getLogger(DistributedThrottler.class);

    private static Jedis jedis = new Jedis("10.0.0.3");

    //The name of the redis resource used to do the locking
    private static String lockRes = "redlock" ;

    //This keeps the time where the api was called last
    private static String lastTimeCalledRes = "lastapicall" ;

    private static int lockExpirationInSeconds = 5 ;

    private Random random = new Random() ;

    private Long randomValue ;

    private long millisBetweenRequests ;

    private long lastTimeCalled = 0;

    public DistributedThrottler(int requestsPer10Seconds, int requestsPer10Minutes) {
        millisBetweenRequests = Double.valueOf(Math.max(10.0/requestsPer10Seconds, 600.0/requestsPer10Minutes) * 1000).longValue();
    }

    @Override
    public void waitFor() {
        //Generate a random value to store as a lockRes
        randomValue = random.nextLong() ;
        String result = jedis.set(lockRes, randomValue.toString(), "NX", "EX", lockExpirationInSeconds) ;

        //We did not acquire the lockRes
        while (result == null) {
            //log.debug("Waiting for lock...");
            try {
                //Sleep for some random time
                Thread.sleep(random.nextInt(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            result = jedis.set(lockRes, randomValue.toString(), "NX", "EX", lockExpirationInSeconds) ;
        }
        log.debug("Got lock");

        //Get the last time called
        String lastTimeCalledString = jedis.get(lastTimeCalledRes) ;
        if (lastTimeCalledString == null) {
            lastTimeCalled = 0;
        } else {
            lastTimeCalled = Long.parseLong(lastTimeCalledString);
        }
        sleepUntilAllowedToCall();
    }

    private void sleepUntilAllowedToCall() {
        while (System.currentTimeMillis() - lastTimeCalled < millisBetweenRequests) {
            try {
                long sleepTime = millisBetweenRequests + lastTimeCalled - System.currentTimeMillis() ;
                log.debug("Can't call API yet, sleeping for {} ms", sleepTime);
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                log.error("This code shouldn't be reached");
                throw new RuntimeException("This code shouldn't be reached");
            }
        }
    }

    public void releaseLock(long lastTimeCalled) {
        String lockValue = jedis.get(lockRes) ;
        if (lockValue != null && Long.parseLong(lockValue) == randomValue) {
            jedis.set(lastTimeCalledRes, String.valueOf(lastTimeCalled)) ;
            jedis.del(lockRes) ;
            log.debug("Released lock");
        } else {
            log.warn("Lock is null or not mine");
        }
    }

    public static void main(String[] args) {
        DistributedThrottler dt = new DistributedThrottler(10, 500) ;
        while (true) {
            dt.waitFor();
            log.debug("Called API");
            dt.releaseLock(0);
        }
    }
}
