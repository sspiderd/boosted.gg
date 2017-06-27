package gg.boosted.posos

import gg.boosted.riotapi.dtos.`match`.{Match, MatchTimeline, Participant}

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
                                 role: String,

                                 matchCreation: Long,
                                 matchDuration: Long,
                                 matchMode: String,
                                 matchType: String,
                                 matchVersion: String,

                                 runes: Map[String, Int],
                                 masteries: Map[String, Int],
                                 startingItems: Seq[String],
                                 itemsBought: Seq[String],
                                 skillsLevelUp: Seq[Int],
                                 friendlies: Seq[MatchParticipant],
                                 foes: Seq[MatchParticipant],
                                 winner: Boolean
                               )

case class MatchParticipant(championId: Int, role: String)


object SummonerMatchSummary {

    def apply(summonerId: Long, md: MatchTimeline): SummonerMatchSummary = {
        val participantIdentity = md.participantIdentities.asScala.filter(_.player.summonerId == summonerId).head
        val participant = md.participants.asScala.filter(_.participantId == participantIdentity.participantId).head
        val participantId = participantIdentity.participantId

        val runesMap = participant.runes.asScala.map(rune => (rune.runeId.toString, rune.rank)).toMap
        val masteriesMap = participant.masteries.asScala.map(mastery => (mastery.masteryId.toString, mastery.rank)).toMap

        val firstItemsBought = ListBuffer[String]()
        val itemsBought = ListBuffer[String]()


        md.timeline.frames.asScala.filter(frame => frame.events != null &&
          frame.events.asScala.filter(_.eventType == "ITEM_PURCHASED").size > 0).head.events.asScala.foreach(event => {
            event.eventType match {
                case "ITEM_PURCHASED" => firstItemsBought += event.itemId.toString
                case "ITEM_UNDO" => {
                    val undoItem = event.itemBefore
                    firstItemsBought.remove(firstItemsBought.lastIndexOf(undoItem))
                }
        }})

        md.timeline.frames.asScala.foreach(frame => {
            if (frame.events != null) {
                frame.events.asScala.filter(event => event.participantId == participantId &&
                    (event.eventType == "ITEM_PURCHASED" || (event.eventType == "ITEM_UNDO" && event.itemBefore != 0)))
                    .foreach(event => event.eventType match {
                        case "ITEM_PURCHASED" => itemsBought += event.itemId.toString
                        case "ITEM_UNDO" => {
                            val undoItem = event.itemBefore
                            itemsBought.remove(itemsBought.lastIndexOf(undoItem))
                        }
                    })
            }
        })

        val skillsLevelUp = md.timeline.frames.asScala.filter(_.events != null).flatMap(_.events.asScala)
            .filter(event => event.participantId == participantId && event.eventType == "SKILL_LEVEL_UP").map(_.skillSlot)


        val friendlies = md.participants.asScala.filter(participantInGame => participantInGame.teamId == participant.teamId &&
            participantInGame.participantId != participantId)
            .map(p => MatchParticipant(p.championId, roleForParticipant(p)))

        val foes = md.participants.asScala.filter(participantInGame => participantInGame.teamId != participant.teamId)
            .map(p => MatchParticipant(p.championId, roleForParticipant(p)))

        new SummonerMatchSummary(
            md.matchId,
            summonerId,
            participantIdentity.player.summonerName,
            md.region,
            participant.championId,
            roleForParticipant(participant),

            md.matchCreation,
            md.matchDuration,
            md.matchMode,
            md.matchType,
            md.matchVersion,

            runesMap,
            masteriesMap,
            firstItemsBought,
            itemsBought,
            skillsLevelUp,
            friendlies,
            foes,
            participant.stats.winner
        )
    }

    def roleForParticipant(p: Participant): String = {
        val lane: String = p.timeline.lane
        var role: String = null
        if (lane == "TOP" || lane == "MIDDLE" || lane == "JUNGLE") {
            role = lane
        }
        else if (lane == "BOTTOM") {
            //This is bot lane
            if (p.timeline.role == "DUO_CARRY") {
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
