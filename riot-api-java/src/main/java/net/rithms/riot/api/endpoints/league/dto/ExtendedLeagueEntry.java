package net.rithms.riot.api.endpoints.league.dto;

/**
 *
 * This is an object that will try to make sense of the shit riot is sending us back with
 * their league api
 *
 * Created by ilan on 9/9/16.
 */
public class ExtendedLeagueEntry {

    public String name;
    public String participantId;
    public String queue;
    public String tier;

    public String division;
    public boolean isFreshBlood;
    public boolean isHotStreak;
    public boolean isInactive;
    public boolean isVeteran;
    public int leaguePoints;
    public int losses;
    public MiniSeries miniSeries;
    public String playerOrTeamId;
    public String playerOrTeamName;
    public int wins;

    public ExtendedLeagueEntry(League league) {
        name = league.getName() ;
        queue = league.getQueue() ;
        tier = league.getTier() ;
        participantId = league.getParticipantId() ;
        if (league.getEntries().size() > 1) {
            throw new RuntimeException("I did not account for this") ;
        }
        LeagueEntry entry = league.getEntries().get(0) ;
        division = entry.getDivision() ;
        isFreshBlood = entry.isFreshBlood() ;
        isHotStreak = entry.isHotStreak() ;
        isInactive = entry.isInactive() ;
        isVeteran = entry.isVeteran() ;
        leaguePoints = entry.getLeaguePoints() ;
        losses = entry.getLosses() ;
        miniSeries = entry.getMiniSeries() ;
        playerOrTeamId = entry.getPlayerOrTeamId() ;
        playerOrTeamName = entry.getPlayerOrTeamName() ;
        wins = entry.getWins() ;
    }
}
