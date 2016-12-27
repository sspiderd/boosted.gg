package gg.boosted.riotapi.utilities;

import gg.boosted.riotapi.dtos.MatchSummary;
import gg.boosted.riotapi.dtos.match.MatchDetail;

/**
 * Created by ilan on 12/27/16.
 */
public class Matches {

    public static MatchSummary detailToSummary(MatchDetail md) {
        MatchSummary ms = new MatchSummary();
        ms.mapId = md.mapId;
        ms.matchCreation = md.matchCreation;
        ms.matchDuration = md.matchDuration;
        ms.matchId = md.matchId ;
        ms.platformId = md.platformId;
        ms.queueType = md.queueType;
        ms.region = md.region;
        ms.season = md.season;




        return ms ;
    }
}
