package gg.boosted.analyzers

import gg.boosted.configuration.Configuration
import gg.boosted.dal.{SummonerMatches, RedisStore}
import gg.boosted.maps.{Champions, Items}
import gg.boosted.posos.{BoostedSummoner, SummonerMatchId, Mindset}
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
