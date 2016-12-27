package gg.boosted.riotapi.utilities;

import gg.boosted.riotapi.dtos.MatchSummary;
import gg.boosted.riotapi.dtos.MatchSummary.Summoner;
import gg.boosted.riotapi.dtos.match.MatchDetail;
import gg.boosted.riotapi.dtos.match.Participant;

import java.util.HashMap;
import java.util.Map;

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
        ms.matchMode = md.matchMode ;
        ms.matchType = md.matchType;
        ms.matchVersion = md.matchVersion;

        Map<Integer, Long> participantsToSummonerIds = participantsToSummoners(md);
        Map<Integer, Summoner> participantsToSummonerObjects = new HashMap<>();
        md.participants.forEach(participant -> {
            Summoner summoner = new Summoner();
            participantsToSummonerObjects.put(participant.participantId, summoner);
            //Add the summoner to his team (currently teams are 100 and 200)
            if (participant.teamId == 100) {
                ms.team1.summoners.add(summoner) ;
            } else {
                ms.team2.summoners.add(summoner) ;
            }
            summoner.summonerId = participantsToSummonerIds.get(participant.participantId);
            summoner.championId = participant.championId;
            summoner.role = roleForParticipant(participant);
            summoner.winner = participant.stats.winner;


        });

        //Items bought
        md.timeline.frames.forEach(frame -> {
            if (frame.events != null) {
                frame.events.forEach(event -> {
                    if (event.itemId != 0) {
                        participantsToSummonerObjects.get(event.participantId).itemsBought.add(event.itemId);
                    }
                });
            }
        });

        return ms ;
    }

    public static Map<Integer, Long> participantsToSummoners(MatchDetail md) {
        Map<Integer, Long> participantsToSummoners = new HashMap<>();
        md.participantIdentities.forEach(participant -> participantsToSummoners.put(participant.participantId, participant.player.summonerId));
        return participantsToSummoners ;
    }

    public static String roleForParticipant(Participant p) {
        String lane = p.timeline.lane;
        String role ;
        if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
            role = lane;
        } else if (lane == "BOTTOM") {
            //This is bot lane
            if (p.timeline.role ==  "DUO_CARRY") {
                role = "BOTTOM";
            } else {
                role = "SUPPORT";
            }
        } else throw new RuntimeException("I Don't know this lane " + lane + " !!");
        return role ;
    }
}
