package gg.boosted.posos

import gg.boosted.riotapi.dtos.SummonerMatchDetails
import gg.boosted.riotapi.dtos.`match`.{MatchDetail, Participant, ParticipantIdentity}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by ilan on 1/4/17.
  */
case class SummonerMatchSummary(
                                   matchId: Long,
                                   summonerId: Long,
                                   summonerName: String,
                                   region: String,
                                   championId: Int,
                                   roleId: Int,

                                   matchCreation: Long,
                                   matchDuration: Long,
                                   matchMode:String,
                                   matchType:String,
                                   matchVersion:String,

                                   runes: Map[Int, Int],
                                   masteries: Map[Int, Int],
                                   itemsBought: Seq[Int],
                                   skillsLevelUp:Seq[Int],
                                   friendlies: Seq[MatchParticipant],
                                   foes:Seq[MatchParticipant],
                                   winner: Boolean
                               )

case class MatchParticipant(summonerId:Long, championId:Int, roleId:Int)


object SummonerMatchSummary {

  def apply(summonerId:Long, md:MatchDetail):SummonerMatchSummary = {
    val participantIdentity = md.participantIdentities.asScala.filter(_.player.summonerId == summonerId).head
    val participant = md.participants.asScala.filter(_.participantId == participantIdentity.participantId).head
    val participantId = participantIdentity.participantId

    val runesMap = participant.runes.asScala.map(rune => (rune.runeId, rune.rank)).toMap
    val masteriesMap = participant.masteries.asScala.map(mastery => (mastery.masteryId, mastery.rank)).toMap

    var itemsBought = ListBuffer[Int]()

    md.timeline.frames.asScala.foreach(frame => {
      if (frame.events != null) {
        frame.events.asScala.filter(event => event.participantId == participantId &&
          event.eventType == "ITEM_PURCHASED" || (event.eventType == "ITEM_UNDO" && event.itemBefore != 0))
          .foreach(event => event.eventType match {
            case "ITEM_PURCHASED" => itemsBought += event.itemId
            case "ITEM_UNDO" => {
              val undoItem = event.itemBefore
              itemsBought.remove (itemsBought.lastIndexOf (undoItem) )
            }
          })
      }
    })

    val skillsLevelUp = md.timeline.frames.asScala.foreach(frame =>{
      if (frame.events != null) {
        frame.events.asScala.filter(event => event.participantId == participantId && event.eventType == "SKILL_LEVEL_UP")
          .map(_.skillSlot)
      }
    })

    //Friends and foes
    md.participants.asScala.foreach(participantInGame => {
      SummonerMatchDetails.Summoner other = new SummonerMatchDetails.Summoner(participantInGame.championId, roleForParticipant(participantInGame));
      if (participantInGame.teamId == participant.teamId && participantInGame.participantId != participant.participantId) ms.friendlies.add(other) else {
        ms.foes.add(other);
      }
    })
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
