package gg.boosted.throttlers;

/**
 *
 * An interface that throttles the calls to the riot api
 *
 * Created by ilan on 12/10/16.
 */
public interface IThrottler {

    /**
     * This method should block until it is possible to make a call
     */
    void waitFor() ;

}
