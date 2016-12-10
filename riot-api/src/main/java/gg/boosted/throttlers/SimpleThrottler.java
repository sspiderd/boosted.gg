package gg.boosted.throttlers;

/**
 *
 * A simple, non-threadsafe throttler
 *
 * Created by ilan on 12/10/16.
 */
public class SimpleThrottler implements IThrottler {

    private long millisBetweenRequests ;

    private long lastTimeCalled = 0;

    public SimpleThrottler(int requestsPer10Seconds, int requestsPer10Minutes) {
        millisBetweenRequests = Double.valueOf(Math.max(requestsPer10Seconds/10.0, requestsPer10Minutes/600.0) * 1000).longValue();
    }

    /**
     * I could probably make this better....
     */
    @Override
    public void waitFor() {
        while (System.currentTimeMillis() - lastTimeCalled < millisBetweenRequests) {
            try {
                Thread.sleep(millisBetweenRequests);
                lastTimeCalled = System.currentTimeMillis() ;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
