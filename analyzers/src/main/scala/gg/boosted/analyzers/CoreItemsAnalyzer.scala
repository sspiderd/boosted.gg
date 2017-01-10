package gg.boosted.analyzers

import gg.boosted.configuration.Configuration
import gg.boosted.dal.RedisStore
import gg.boosted.maps.{Champions, Items}
import gg.boosted.posos.{BoostedSummoner, MatchId, Mindset}
import gg.boosted.riotapi.dtos.MatchSummary
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.utils.{GeneralUtils, JsonUtil}
import gg.boosted.{Application, Role}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by ilan on 1/10/17.
  */
object CoreItemsAnalyzer {

    val log = LoggerFactory.getLogger(CoreItemsAnalyzer.getClass)


    def boostedSummonersToWeightedMatchSummary(ds: Dataset[BoostedSummoner]):DataFrame = {

        import Application.session.implicits._

        //Download the matches from the dataset
        ds.foreachPartition(partitionOfRecords => {
            var matchIds = new mutable.HashSet[MatchId]

            partitionOfRecords.foreach(row => row.matches.foreach(
                _match => matchIds += MatchId(_match, Region.valueOf(row.region))))

            //Get all unknown matches
            val unknownMatches = new mutable.HashSet[MatchId]
            matchIds.foreach(id => RedisStore.getMatch(id).getOrElse(unknownMatches += id))

            unknownMatches.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                ids.foreach(id => RedisStore.addMatch(id, api.getMatchSummaryAsJson(id.id)))
            })
        })

        ds.foreachPartition(_ => Items.populateMapIfEmpty())
        import org.apache.spark.sql.functions._

        var columnNames = Seq[String]("matchId", "summonerId", "region", "championId", "roleId", "winner", "coreItems")

        val sm = ds.flatMap(row => {

            val summonerId = row.summonerId
            val region = row.region
            val championId = row.championId
            val role = row.role

            row.matches.map(matchId => {

                val matchSummary = JsonUtil.fromJson[MatchSummary](RedisStore.getMatch(MatchId(matchId, Region.valueOf(region))).get)

                val summonerFromSummary = matchSummary.team1.summoners.asScala.find(summoner => summoner.summonerId == summonerId)
                    .getOrElse(matchSummary.team2.summoners.asScala.find(summoner => summoner.summonerId == summonerId).get)

                //Get the X (2) legendaryItems
                val numberOfCoreItems = Configuration.getInt("number.of.core.items")
                val coreItems = summonerFromSummary.itemsBought.asScala.map(_.toInt)
                    .map(Items.items.get(_).get).filter(_.gold >= Items.legendaryCutoff).take(numberOfCoreItems).map(_.id)

                (matchId, summonerId, region, championId, Role.valueOf(role).roleId, if (summonerFromSummary.winner) 1 else 0,
                    coreItems)

                //                WeightedSummonerMatchSummary.create(matchId, summonerId, championId, Role.valueOf(role).roleId,
                //                    summonerFromSummary.winner, region, 0, summonerFromSummary.itemsBought.asScala.map(_.toInt).toList, 2)
            })
        }).toDF(columnNames:_*)
            .where(size(col("coreItems")) >= Configuration.getInt("number.of.core.items"))
        sm.createOrReplaceTempView("SummonerMatchSummary")
        //This here will return the number of games played with the core items for each champion/role combo
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

    def cluster(summonerMatchSummaryWithWeights:DataFrame):Dataset[Mindset] = {
        import Application.session.implicits._
        summonerMatchSummaryWithWeights.createOrReplaceTempView("WeightedSummary")
        val championRolesPairs = summonerMatchSummaryWithWeights.select("championId", "roleId").distinct().collect()
        val repartitioned = summonerMatchSummaryWithWeights.repartition(championRolesPairs.size, $"championId", $"roleId").cache()
        var unioned = repartitioned.sqlContext.createDataset(Application.session.sparkContext.emptyRDD[Mindset])
        championRolesPairs.foreach(pair => {
            unioned = GeneralUtils.time(championRoleItemsKMeans(pair.getInt(0), pair.getInt(1), repartitioned).union(unioned),
                s"clustering for ${Champions.byId(pair.getInt(0))}:${Role.byId(pair.getInt(1)).toString}")
        })
        unioned
    }

    def championRoleItemsKMeans(championId:Int, roleId:Int, dataset: DataFrame):Dataset[Mindset] = {

        import Application.session.implicits._
        val df = new VectorAssembler()
            .setInputCols(Array(
                "ad", "ap", "armor", "mr", "health", "mana", "healthRegen", "manaRegen", "criticalStrikeChance", "as",
                "flatMS", "lifeSteal", "percentMS"
            ))
            .setOutputCol("features").transform(dataset.filter(s"championId = ${championId} and roleId = ${roleId}"))

        log.debug(s"Calculating for champion ${Champions.byId(championId)} and role ${Role.byId(roleId)}")
        // Trains a k-means model. We use 3 groups
        val model = new KMeans().setK(3).fit(df)

        val predictions = model.summary.predictions

        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        //  val WSSSE = model.computeCost(df)
        //  println(s"Within Set Sum of Squared Errors = $WSSSE")

        predictions.createOrReplaceTempView(s"kmeansPredictionsFor_${championId}_${roleId}")
        predictions.sparkSession.sql(
            s"""SELECT championId, roleId, prediction as cluster, coreItems, avg(winner) as winrate, count(*) as gamesPlayed, collect_list(concat(summonerId, ':', matchId)) as summonerMatches
               |FROM kmeansPredictionsFor_${championId}_${roleId}
               |GROUP BY championId, roleId, prediction, coreItems
               |HAVING gamesPlayed > 0
               |ORDER BY cluster, winrate DESC, gamesPlayed DESC
             """.stripMargin).as[Mindset]
    }

}
