package gg.boosted.riotapi.utilities;

import gg.boosted.riotapi.dtos.SummonerMatchDetails;
import gg.boosted.riotapi.dtos.match.MatchDetail;
import gg.boosted.riotapi.dtos.match.Participant;
import gg.boosted.riotapi.dtos.match.ParticipantIdentity;

import java.util.stream.Collectors;

/**
 * Created by ilan on 12/27/16.
 */
public class Matches {

    public static SummonerMatchDetails detailToSummonerSummary(long summonerId, MatchDetail md) {
        SummonerMatchDetails ms = new SummonerMatchDetails();

        ms.summonerId = summonerId;
        ms.mapId = md.mapId;
        ms.matchCreation = md.matchCreation;
        ms.matchDuration = md.matchDuration;
        ms.matchId = md.matchId;
        ms.platformId = md.platformId;
        ms.queueType = md.queueType;
        ms.region = md.region;
        ms.season = md.season;
        ms.matchMode = md.matchMode;
        ms.matchType = md.matchType;
        ms.matchVersion = md.matchVersion;

        //Match our summoner to it's equivalent participant id
        ParticipantIdentity participantIdentity = md.participantIdentities.stream().filter(pi -> pi.player.summonerId == summonerId).findFirst().get();
        Participant participant = md.participants.stream().filter(p -> p.participantId == participantIdentity.participantId).findFirst().get();

        int participantId = participantIdentity.participantId;

        ms.summonerName = participantIdentity.player.summonerName;

        ms.winner = participant.stats.winner;

        ms.runes = participant.runes;
        ms.masteries = participant.masteries;


        md.timeline.frames.forEach(frame -> {
            if (frame.events != null) {
                //Items bought
                frame.events.stream().filter(event -> event.participantId == participantId &&
                        (event.eventType.equals("ITEM_PURCHASED") ||
                                (event.eventType.equals("ITEM_UNDO") && event.itemBefore != 0)))
                        .forEach(event -> {
                            switch (event.eventType) {
                                case "ITEM_PURCHASED":
                                    ms.itemsBought.add(event.itemId);
                                    break;
                                case "ITEM_UNDO": {
                                    Integer undoItem = event.itemBefore;
                                    ms.itemsBought.remove(ms.itemsBought.lastIndexOf(undoItem));
                                }
                            }
                        });

                //Skills
                ms.skillLevelUp = frame.events.stream()
                        .filter(event -> event.participantId == participantId && event.eventType.equals("SKILL_LEVEL_UP"))
                        .map(event -> event.skillSlot).collect(Collectors.toList());
            }
        });

        //Friends and foes
        md.participants.stream().forEach(participantInGame -> {
            SummonerMatchDetails.Summoner other = new SummonerMatchDetails.Summoner(participantInGame.championId, roleForParticipant(participantInGame));
            if (participantInGame.teamId == participant.teamId && participantInGame.participantId != participant.participantId) {
                ms.friendlies.add(other);
            } else {
                ms.foes.add(other);
            }

        });


        return ms;
    }


    public static String roleForParticipant(Participant p) {
        String lane = p.timeline.lane;
        String role;
        if (lane.equals("TOP") || lane.equals("MIDDLE") || lane.equals("JUNGLE")) {
            role = lane;
        } else if (lane.equals("BOTTOM")) {
            //This is bot lane
            if (p.timeline.role == "DUO_CARRY") {
                role = "BOTTOM";
            } else {
                role = "SUPPORT";
            }
        } else throw new RuntimeException("I Don't know this lane " + lane + " !!");
        return role;
    }
}

//    public static MatchSummary detailToSummary(MatchDetail md) {
//        MatchSummary ms = new MatchSummary();
//        ms.mapId = md.mapId;
//        ms.matchCreation = md.matchCreation;
//        ms.matchDuration = md.matchDuration;
//        ms.matchId = md.matchId ;
//        ms.platformId = md.platformId;
//        ms.queueType = md.queueType;
//        ms.region = md.region;
//        ms.season = md.season;
//        ms.matchMode = md.matchMode ;
//        ms.matchType = md.matchType;
//        ms.matchVersion = md.matchVersion;
//
//        Map<Integer, Long> participantsToSummonerIds = participantsToSummoners(md);
//        Map<Integer, Summoner> participantsToSummonerObjects = new HashMap<>();
//        md.participants.forEach(participant -> {
//            Summoner summoner = new Summoner();
//            participantsToSummonerObjects.put(participant.participantId, summoner);
//            //Add the summoner to his team (currently teams are 100 and 200)
//            if (participant.teamId == 100) {
//                ms.team1.summoners.add(summoner) ;
//            } else {
//                ms.team2.summoners.add(summoner) ;
//            }
//            summoner.summonerId = participantsToSummonerIds.get(participant.participantId);
//            summoner.championId = participant.championId;
//            summoner.role = roleForParticipant(participant);
//            summoner.winner = participant.stats.winner;
//            summoner.runes = participant.runes;
//            summoner.masteries = participant.masteries;
//
//
//        });
//
//        //Items bought
//        md.timeline.frames.forEach(frame -> {
//            if (frame.events != null) {
//                frame.events.forEach(event -> {
//                    if (event.eventType.equals("ITEM_PURCHASED")) {
//                        participantsToSummonerObjects.get(event.participantId).itemsBought.add(event.itemId);
//                    } else if (event.eventType.equals("ITEM_UNDO") && event.itemBefore != 0) {
//                        List<Integer> itemsBought = participantsToSummonerObjects.get(event.participantId).itemsBought;
//                        Integer undoItem = event.itemBefore ;
//                        itemsBought.remove(itemsBought.lastIndexOf(undoItem));
//                    }
//                });
//            }
//        });
//
//        return ms ;
//    }
//
//    public static Map<Integer, Long> participantsToSummoners(MatchDetail md) {
//        Map<Integer, Long> participantsToSummoners = new HashMap<>();
//        md.participantIdentities.forEach(participant -> participantsToSummoners.put(participant.participantId, participant.player.summonerId));
//        return participantsToSummoners ;
//    }

