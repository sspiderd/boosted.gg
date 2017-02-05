package gg.boosted.analyzers

import gg.boosted.Application.session.implicits._
import gg.boosted.configuration.Configuration
import gg.boosted.maps.{Champions, Items}
import gg.boosted.posos.{Mindset, SummonerMatchSummary}
import gg.boosted.utils.GeneralUtils
import gg.boosted.{Application, Role}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

/**
  *
  * This class will attempt to cluster summonerMatchSummaries by values of their core items (that is, the first 2 items
  * that the summoner bought in the game)
  *
  * Created by ilan on 1/10/17.
  */
object CoreItemsClusterer {

    val log = LoggerFactory.getLogger(CoreItemsClusterer.getClass)

    def weightItems(summaries: Dataset[SummonerMatchSummary]): DataFrame = {
        import org.apache.spark.sql.functions._

        val columnNames = Seq[String]("matchId", "summonerId", "region", "championId", "roleId", "winner", "coreItems")

        val sm = summaries.map(row => {

            val matchId = row.matchId
            val summonerId = row.summonerId
            val region = row.region
            val championId = row.championId
            val role = row.role.toString

            //Get the X (2) legendaryItems
            val numberOfCoreItems = Configuration.getInt("number.of.core.items")
            val coreItems = row.itemsBought
              .map(Items.items()(_)).filter(_.gold >= Items.legendaryCutoff).take(numberOfCoreItems).map(_.id)

            (matchId, summonerId, region, championId, Role.valueOf(role).roleId, if (row.winner) 1 else 0,
              coreItems)

        }).toDF(columnNames: _*)
          .where(size(col("coreItems")) >= Configuration.getInt("number.of.core.items"))
        sm.createOrReplaceTempView("SummonerMatchDetails")
        //This here will return the number of games played with the core runes for each champion/role combo
        val summonerMatchSummary = sm.sqlContext.sql(
            s"""
               |SELECT matchId, summonerId, region, A.championId, A.roleId, winner, A.coreItems, gamesPlayedWithCoreItems
               |FROM (
               |SELECT * FROM SummonerMatchDetails B join (
               |SELECT championId, roleId, coreItems, count(coreItems) as gamesPlayedWithCoreItems
               |FROM SummonerMatchDetails
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

    def cluster(summonerMatchSummaries:Dataset[SummonerMatchSummary]):Dataset[Mindset] = {
        import Application.session.implicits._
        val summonerMatchSummaryWithWeights = weightItems(summonerMatchSummaries)
        summonerMatchSummaryWithWeights.createOrReplaceTempView("WeightedSummary")
        val championRolesPairs = summonerMatchSummaryWithWeights.select("championId", "roleId").distinct().collect()
        //Repartition to the number of cores we have
        val defPar = Application.session.sparkContext.defaultParallelism
        val repartitioned = summonerMatchSummaryWithWeights.repartition(defPar, $"championId", $"roleId").cache()
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

        //We need to find the optimal number of clusters
        //TODO: Implement this
//        for (k <- 1 to 5) {
//            val model = new KMeans().setK(k).fit(df)
//            val WSSSE = model.computeCost(df)
//        }

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
