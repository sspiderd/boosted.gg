package gg.boosted.riotapi.dtos;

import gg.boosted.riotapi.dtos.match.Mastery;
import gg.boosted.riotapi.dtos.match.Rune;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ilan on 1/17/17.
 */
public class SummonerMatchDetails {

    public long matchId;
    public long summonerId;
    public String region;
    public int championId ;
    public int role ;

    public String summonerName ;

    public int mapId;
    public long matchCreation;
    public long matchDuration;
    public String matchMode;
    public String matchType;
    public String matchVersion;
    public String platformId;
    public String queueType;
    public String season;


    public List<Integer> itemsBought = new LinkedList<>();
    public List<Rune> runes = new LinkedList<>();
    public List<Mastery> masteries = new LinkedList<>();
    public List<Integer> skillLevelUp = new LinkedList<>() ;
    public List<Summoner> friendlies = new LinkedList<>();
    public List<Summoner> foes = new LinkedList<>();
    public boolean winner = false;
    public int wardsPlaced ;

    public static class Summoner {

        public int championId ;
        public String role ;

        public Summoner(int championId, String role) {
            this.championId = championId ;
            this.role = role ;
        }
    }

}
