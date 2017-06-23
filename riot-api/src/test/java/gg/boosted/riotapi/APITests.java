package gg.boosted.riotapi;

import gg.boosted.riotapi.dtos.MatchReference;
import gg.boosted.riotapi.dtos.Summoner;
import gg.boosted.riotapi.throttlers.SimpleThrottler;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by Angel on 09-Jun-17.
 */
public class APITests {

    Logger log = LoggerFactory.getLogger(APITests.class) ;

    RiotApi api = new RiotApi(Region.EUW1) ;

    @Before
    public void before() {
        api.throttler = new SimpleThrottler(10, 500) ;
    }

    @Test
    public void test1() throws IOException {
        Long challenger = api.getChallengersIds().get(0) ;
        log.info("Challenger id found: " + challenger);
        Summoner s = api.getSummoner(challenger) ;
        log.info("Account for summoner " + challenger + " is " + s.accountId);
        List<MatchReference> matches = api.getMatchList(s.accountId, 11);
        api.getMatch(matches.get(0).gameId) ;
    }

    public void test2() throws IOException {
        RiotApi r = new RiotApi(Region.EUW1) ;
        r.throttler = new SimpleThrottler(10, 500) ;
        Long challenger = r.getChallengersIds().get(0) ;
        log.info("Challenger id found: " + challenger);
        Summoner s = r.getSummoner(challenger) ;
        log.info("Account for summoner " + challenger + " is " + s.accountId);
        List<MatchReference> matches = r.getMatchList(s.accountId, 11);
    }

}
