package gg.boosted.riotapi.dtos;

/**
 * Created by ilan on 12/12/16.
 */
public class LeaguePosition {

    public boolean freshBlood;
    public boolean hotStreak;
    public boolean inactive;
    public String leagueName;
    public int leaguePoints;
    public int losses;
    public MiniSeries miniSeries;
    public String playerOrTeamId;
    public String playerOrTeamName;
    public String queueType;
    public String rank;
    public String tier;
    public boolean veteran;
    public int wins;
    public static class MiniSeries {

        public int losses;
        public String progress;
        public int target;
        public int wins;

    }

}
