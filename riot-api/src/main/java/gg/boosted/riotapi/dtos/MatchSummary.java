package gg.boosted.riotapi.dtos;

import java.util.List;

/**
 * Created by ilan on 12/27/16.
 */
public class MatchSummary {

    public int mapId;
    public long matchCreation;
    public long matchDuration;
    public long matchId;
    public String matchMode;
    public String matchType;
    public String matchVersion;
    public String platformId;
    public String queueType;
    public String region;
    public String season;
    public Team team1 ;
    public Team team2 ;



    public static class Team {

        public boolean winner ;
        public List<Summoner> summoners ;

    }

    public static class Summoner {

        public long summonerId;
        public int championId ;
        public String role ;
        public List<Integer> itemsBought ;
        public int wardsPlaced ;
    }

}
