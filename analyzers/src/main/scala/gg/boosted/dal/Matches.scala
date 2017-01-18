package gg.boosted.dal

import gg.boosted.riotapi.dtos.SummonerMatchDetails
import gg.boosted.riotapi.dtos.`match`.{MatchDetail, Participant, ParticipantIdentity}
import scala.collection.JavaConverters._

/**
  * Created by ilan on 1/18/17.
  */
object Matches {

  def detailToSummonerSummary(summonerId: Long, md: MatchDetail): SummonerMatchDetails = {
    val ms: SummonerMatchDetails = new SummonerMatchDetails
    ms.summonerId = summonerId
    ms.mapId = md.mapId
    ms.matchCreation = md.matchCreation
    ms.matchDuration = md.matchDuration
    ms.matchId = md.matchId
    ms.platformId = md.platformId
    ms.queueType = md.queueType
    ms.region = md.region
    ms.season = md.season
    ms.matchMode = md.matchMode
    ms.matchType = md.matchType
    ms.matchVersion = md.matchVersion
    //Match our summoner to it's equivalent participant id
    val participantIdentity: ParticipantIdentity = md.participantIdentities.stream.filter(pi -> pi.player.summonerId == summonerId).findFirst.get
    val participant: Participant = md.participants.stream.filter(p -> p.participantId == participantIdentity.participantId).findFirst.get
    val participantId: Int = participantIdentity.participantId
    ms.summonerName = participantIdentity.player.summonerName
    ms.winner = participant.stats.winner
    ms.runes = participant.runes
    ms.masteries = participant.masteries
    md.timeline.frames.asScala.foreach(frame => {
      if (frame.events != null) {
        //Items bought
        frame.events.asScala.filter(event => event.participantId == participantId &&
          (event.eventType.equals("ITEM_PURCHASED") ||
            (event.eventType.equals("ITEM_UNDO") && event.itemBefore != 0)))
          .foreach(event => {
            event.eventType match {
              case "ITEM_PURCHASED" => ms.itemsBought.add(event.itemId)
              case "ITEM_UNDO" => {
                val undoItem = event.itemBefore
                ms.itemsBought.remove (ms.itemsBought.lastIndexOf (undoItem) )
              }
            }
          });

        //Skills
        ms.skillLevelUp = frame.events.asScala
          .filter(event => event.participantId == participantId && event.eventType.equals("SKILL_LEVEL_UP"))
          .map(event => event.skillSlot).asJava
      }
    })
    //Friends and foes
    md.participants.stream.forEach(participantInGame => {
      SummonerMatchDetails.Summoner other = new SummonerMatchDetails.Summoner(participantInGame.championId, roleForParticipant(participantInGame));
      if (participantInGame.teamId == participant.teamId && participantInGame.participantId != participant.participantId) ms.friendlies.add(other) else {
        ms.foes.add(other);
      }
    })
    return ms
  }

  def roleForParticipant(p: Participant): String = {
    val lane: String = p.timeline.lane
    var role: String = null
    if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
      role = lane
    }
    else if (lane == "BOTTOM") {
      //This is bot lane
      if (p.timeline.role eq "DUO_CARRY") {
        role = "BOTTOM"
      }
      else {
        role = "SUPPORT"
      }
    }
    else throw new RuntimeException("I Don't know this lane " + lane + " !!")
    return role
  }

}
