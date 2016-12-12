package gg.boosted.riotapi.throttlers;

/**
 * This throttler is based on the "redlock" algorithm (https://redis.io/topics/distlock)
 * It is used to coordinate API calls between several processes using redis as a lock mechanism
 *
 * Created by ilan on 12/12/16.
 */
public class DistributedThrottler implements IThrottler{


    @Override
    public void waitFor() {

    }
}
