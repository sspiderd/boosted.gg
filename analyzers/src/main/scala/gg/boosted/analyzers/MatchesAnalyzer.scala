package gg.boosted.analyzers

import gg.boosted.Role
import gg.boosted.configuration.Configuration
import gg.boosted.dal.Matches
import gg.boosted.maps.Items
import gg.boosted.posos.{BoostedSummoner, SummonerMatchSummary}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import gg.boosted.Application.session.implicits._

/**
  * Created by ilan on 1/15/17.
  */
object MatchesAnalyzer {

    val log = LoggerFactory.getLogger(MatchesAnalyzer.getClass)

    def boostedSummonersToSummaries(boostedSummoners: Dataset[BoostedSummoner]): Dataset[SummonerMatchSummary] = {
        Matches.populateMatches(boostedSummoners.toDF())

        boostedSummoners.flatMap(row => {

            val summonerId = row.summonerId
            val region = row.region
            val championId = row.championId
            val roleId = row.roleId

            row.matches.map(matchId => {
                val matchSummary = Matches.get(matchId, region)

                val summonerFromSummary = matchSummary.team1.summoners.asScala.find(summoner => summoner.summonerId == summonerId)
                    .getOrElse(matchSummary.team2.summoners.asScala.find(summoner => summoner.summonerId == summonerId).get)

                SummonerMatchSummary(matchId, summonerId, region, championId, roleId,
                    summonerFromSummary.runes.asScala.map(rune => (rune.runeId, rune.rank)).toMap,
                    summonerFromSummary.masteries.asScala.map(mastery => (mastery.masteryId, mastery.rank)).toMap,
                    summonerFromSummary.itemsBought.asScala.map(_.toInt),
                    summonerFromSummary.winner)
            })
        })
    }

    def summariesToWeightedSummaries(summaries: Dataset[SummonerMatchSummary]): DataFrame = {
        import org.apache.spark.sql.functions._

        var columnNames = Seq[String]("matchId", "summonerId", "region", "championId", "roleId", "winner", "coreItems")

        val sm = summaries.map(row => {

            val matchId = row.matchId
            val summonerId = row.summonerId
            val region = row.region
            val championId = row.championId
            val role = Role.byId(row.roleId).toString

            //Get the X (2) legendaryItems
            val numberOfCoreItems = Configuration.getInt("number.of.core.items")
            val coreItems = row.itemsBought
                .map(Items.items.get(_).get).filter(_.gold >= Items.legendaryCutoff).take(numberOfCoreItems).map(_.id)

            (matchId, summonerId, region, championId, Role.valueOf(role).roleId, if (row.winner) 1 else 0,
                coreItems)

        }).toDF(columnNames: _*)
            .where(size(col("coreItems")) >= Configuration.getInt("number.of.core.items"))
        sm.createOrReplaceTempView("SummonerMatchSummary")
        //This here will return the number of games played with the core runes for each champion/role combo
        val summonerMatchSummary = sm.sqlContext.sql(
            s"""
               |SELECT matchId, summonerId, region, A.championId, A.roleId, winner, A.coreItems, gamesPlayedWithCoreItems
               |FROM (
               |SELECT * FROM SummonerMatchSummary B join (
               |SELECT championId, roleId, coreItems, count(coreItems) as gamesPlayedWithCoreItems
               |FROM SummonerMatchSummary
               |GROUP BY championId, roleId, coreItems
               |) A on A.championId = B.championId and A.roleId = B.roleId and A.coreItems = B.coreItems
               |WHERE gamesPlayedWithCoreItems >= ${Configuration.getInt("min.number.of.games.played.with.core.items")}
               |)
             """.stripMargin)
        //Add weights...
        summonerMatchSummary.map(row => {
            val coreItems = row.getSeq[Int](6)
            val aw = coreItems.map(Items.byId).map(Items.weights).reduce(Items.accumulatedWeight)
            (row.getLong(0), row.getLong(1), row.getString(2), row.getInt(3), row.getInt(4), row.getInt(5), coreItems, row.getLong(7),
                aw.attackDamage, aw.abilityPower, aw.armor, aw.magicResistance, aw.health, aw.mana, aw.healthRegen, aw.manaRegen,
                aw.criticalStrikeChance, aw.attackSpeed, aw.flatMovementSpeed, aw.lifeSteal, aw.percentMovementSpeed)
        }).toDF("matchId", "summonerId", "region", "championId", "roleId", "winner", "coreItems", "gamesPlayedWithCoreItems",
            "ad", "ap", "armor", "mr", "health", "mana", "healthRegen", "manaRegen", "criticalStrikeChance", "as", "flatMS", "lifeSteal",
            "percentMS")
    }

}
