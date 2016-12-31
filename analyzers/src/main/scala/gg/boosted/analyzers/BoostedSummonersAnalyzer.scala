package gg.boosted.analyzers

import java.util.Date

import com.datastax.spark.connector._
import gg.boosted.dal.RedisStore
import gg.boosted.maps.{Champions, Items}
import gg.boosted.posos._
import gg.boosted.riotapi.dtos.MatchSummary
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.utils.JsonUtil
import gg.boosted.{Application, Role}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by ilan on 8/26/16.
  */
object BoostedSummonersAnalyzer {

    val log = LoggerFactory.getLogger(BoostedSummonersAnalyzer.getClass)

    /**
      *
      * Calculate the Most boosted summoners at each role
      *
      * The calculation should speak for itself, but let's face it, it's kinda hard to read so...
      *
      * The inner subquery
      *
      * @param ds    of type [SummonerMatch]
      * @param minGamesPlayed
      * @param since take into account only games played since that time
      * @return
      */
    def process(ds: Dataset[SummonerMatch], minGamesPlayed: Int, since: Long, maxRank: Int): Unit = {

        log.info(s"Processing dataset with ${minGamesPlayed} games played (min), since '${new Date(since)}' with max rank ${maxRank}")

        val boostedSummoners = findBoostedSummoners(ds, minGamesPlayed, since, maxRank).cache()

        log.info(s"Originally i had ${ds.count()} rows and now i have ${boostedSummoners.count()}")

        //Here i download the the full match profile for the matches i haven't stored in redis yet
        val matchedEvents = boostedSummonersToMatchSummary(boostedSummoners)

        //At this point i am at a fix. I need to get the summoner names and lolscore for all summoners that have gotten to this point.
        //The riot api allows me to get names and league entrries for multiple IDs and i need to do it in order to minimize the calls
        //To the riot api. However, i don't think there's a way to call a function for an entire column so i have to use a trick here...
        getNamesAndLoLScore(boostedSummoners)
        import Application.session.implicits._
        val mapWithNames = boostedSummoners.map(
            r => {
                val summonerId = SummonerId(r.summonerId, Region.valueOf(r.region))
                val summonerName = RedisStore.getSummonerName(summonerId)
                val lolScore = RedisStore.getSummonerLOLScore(summonerId).getOrElse(LoLScore("UNKNOWN", "U", 0))
                val champion = Champions.byId(r.championId)
                //val lastUpdated = new Date()
                (champion, r.role, summonerName, summonerId.region.toString, r.winrate, r.rank, summonerId.id,
                    lolScore.tier, lolScore.division, lolScore.leaguePoints, lolScore.lolScore, r.gamesPlayed, r.matches)
            })
        //TODO: I should delete from cassandra first, but for now it's ok

        mapWithNames.rdd.saveToCassandra("boostedgg", "boosted_summoners",
            SomeColumns("champion", "role", "summoner_name", "region", "winrate", "rank", "summoner_id",
                "tier", "division", "league_points", "lol_score", "games_played", "matches"))

        log.info(s"Saved ${mapWithNames.count()} rows to cassandra")
    }

    /**
      * Returns a raw representation of boosted summoners.
      * That is, without champion id, lol scores and summoner names
      *
      * @param ds
      * @param minGamesPlayed
      * @param since
      * @param maxRank
      * @return
      */
    def findBoostedSummoners(ds: Dataset[SummonerMatch], minGamesPlayed: Int, since: Long, maxRank: Int)
    : Dataset[BoostedSummoner] = {

        //Use "distinct" so that in case a match got in more than once it will count just once
        import Application.session.implicits._
        ds.distinct().createOrReplaceTempView("MostBoostedSummoners");
        ds.sparkSession.sql(
            s"""SELECT championId, roleId, summonerId, region, gamesPlayed, winrate, matches,
               |rank() OVER (PARTITION BY championId, roleId ORDER BY winrate DESC, gamesPlayed DESC, summonerId DESC) as rank FROM (
               |SELECT championId, roleId, summonerId, region, count(*) as gamesPlayed, (sum(if (winner=true,1,0))/count(winner)) as winrate, collect_list(matchId) as matches
               |FROM MostBoostedSummoners
               |WHERE date >= $since
               |GROUP BY championId, roleId, summonerId, region
               |HAVING winrate > 0.5 AND gamesPlayed >= $minGamesPlayed
               |) having rank <= $maxRank
      """.stripMargin)
            //Map from Champion and role ids to their respective names
            .map(r => BoostedSummoner(r.getInt(0), Role.byId(r.getInt(1)).toString, r.getLong(2),
            r.getString(3), r.getLong(4), r.getDouble(5), r.getSeq[Long](6), r.getInt(7)))
    }


    def boostedSummonersToMatchSummary(ds: Dataset[BoostedSummoner])
    : Dataset[SummonerMatchSummary] = {

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

        //Create a map and its inverse
        ds.foreachPartition(partition => {
            Items.populateMapIfEmpty()
        })

        ds.flatMap(row => {

            val summonerId = row.summonerId
            val region = row.region
            val championId = row.championId
            val role = row.role

            row.matches.map(matchId => {

                val matchSummary = JsonUtil.fromJson[MatchSummary](RedisStore.getMatch(MatchId(matchId, Region.valueOf(region))).get)

                val summonerFromSummary = matchSummary.team1.summoners.asScala.find(summoner => summoner.summonerId == summonerId)
                    .getOrElse(matchSummary.team2.summoners.asScala.find(summoner => summoner.summonerId == summonerId).get)

                SummonerMatchSummary(matchId, summonerId, championId, Role.valueOf(role).roleId,
                    summonerFromSummary.winner, region, 0, summonerFromSummary.itemsBought.asScala.map(_.toInt).toList)
            })
        })
    }

    def matchSummaryKMeans(dataset: Dataset[SummonerMatchSummary]): Unit = {

        val ds = dataset.filter(_.chosenItems.size >= 2).cache()
        val championRolesPairs = ds.select("championId", "roleId").distinct().collect()
        championRolesPairs.foreach(pair => {
            val current = ds.filter(row => row.championId == pair.getInt(0) && row.roleId == pair.getInt(1))
            val df = new VectorAssembler()
                .setInputCols(Array(
                    "FlatArmorMod",
                    "FlatAttackSpeedMod",
                    "FlatBlockMod",
                    "FlatCritChanceMod",
                    "FlatCritDamageMod",
                    "FlatEXPBonus",
                    "FlatEnergyPoolMod",
                    "FlatEnergyRegenMod",
                    "FlatHPPoolMod",
                    "FlatHPRegenMod",
                    "FlatMPPoolMod",
                    "FlatMPRegenMod",
                    "FlatMagicDamageMod",
                    "FlatMovementSpeedMod",
                    "FlatPhysicalDamageMod",
                    "FlatSpellBlockMod",
                    "PercentArmorMod",
                    "PercentAttackSpeedMod",
                    "PercentBlockMod",
                    "PercentCritChanceMod",
                    "PercentCritDamageMod",
                    "PercentDodgeMod",
                    "PercentEXPBonus",
                    "PercentHPPoolMod",
                    "PercentHPRegenMod",
                    "PercentLifeStealMod",
                    "PercentMPPoolMod",
                    "PercentMPRegenMod",
                    "PercentMagicDamageMod",
                    "PercentMovementSpeedMod",
                    "PercentPhysicalDamageMod",
                    "PercentSpellBlockMod",
                    "PercentSpellVampMod"
                ))
                .setOutputCol("features").transform(current)


            // Trains a k-means model.
            val kmeans = new KMeans().setK(4).setSeed(1L)
            val model = kmeans.fit(df)

            // Evaluate clustering by computing Within Set Sum of Squared Errors.
            val WSSSE = model.computeCost(df)
            println(s"Calculated champion ${Champions.byId(pair.getInt(0))} and role ${Role.byId(pair.getInt(1))}")
            println(s"Within Set Sum of Squared Errors = $WSSSE")

            model.summary.predictions.show(100)

            model.summary.predictions.select("prediction", "chosenItems").collect().groupBy(_.getInt(0)).foreach(row => {
                println(s"Cluster ${row._1}")
                row._2.map(row => row.getSeq[Int](1)).foreach(items => {
                    items.foreach(item => print(Items.byId(item).name + ":"))
                    println()
                })

            })
            val i = 1
        })
    }


    //    def matchSummaryDecisionTree(ms:Dataset[SummonerMatchSummary]):Unit = {
    //        import Application.session.implicits._
    //
    //        val cached = ms.cache()
    //        val championRolesPairs = cached.select($"championId", $"roleId").distinct().collect()
    //        championRolesPairs.foreach(row => {
    //            val championId = row.getInt(0)
    //            val roleId = row.getInt(1)
    //
    //            val current = cached.filter(row => row.championId == championId && row.roleId == roleId)
    //
    //            //Flatten the items tree
    //            //There are a total of X legendary items, we will flatten by that amount
    //            val legendariesToIndex = Items.legendaries().values.map(_.id.toInt).zipWithIndex.toMap
    //            val indexToLegendaries = legendariesToIndex.map(_.swap)
    //
    //
    //            val expanded = current.map(_match => {
    //                val expandedItemsBoughtList = Array.fill(legendariesToIndex.size){0}
    //
    //                _match.itemsBought.foreach(item => {
    //                    val legendaryIndex = legendariesToIndex.getOrElse(item, -1)
    //                    if (legendaryIndex != -1) expandedItemsBoughtList(legendaryIndex) +=1
    //                })
    //                val winnerNum = if (_match.winner) 1.0 else 0.0
    //                (_match.matchId, _match.summonerId, _match.championId, _match.roleId, winnerNum, _match.region, _match.date, Vectors.dense(expandedItemsBoughtList.map(_.toDouble)))
    //            })
    //            //val legendaryNames = indexToLegendaries.map(r => Items.legendaries()(r._2).name)
    //
    //            val list = List("matchId", "summonerId", "championId", "roleId", "winner", "region", "date", "legendary item ids")
    //            val df = expanded.toDF(list:_*)
    //
    //
    //            val splits = df.randomSplit(Array(0.8, 0.2))
    //            val (trainingData, testData) = (splits(0), splits(1))
    //
    //            val model = new DecisionTreeClassifier()
    //                .setLabelCol("winner")
    //                .setFeaturesCol("legendary item ids")
    //                .setImpurity("entropy")
    //                .setMaxDepth(20)
    //                .setMaxBins(24)
    //                .fit(trainingData)
    //
    //
    //            // Make predictions.
    //            val predictions = model.transform(testData)
    //
    //            // Select (prediction, true label) and compute test error
    //            val evaluator = new MulticlassClassificationEvaluator()
    //                .setLabelCol("winner")
    //                .setPredictionCol("prediction")
    //                .setMetricName("accuracy")
    //            val accuracy = evaluator.evaluate(predictions)
    //            println(s"Test Error for ${Champions.byId(championId)}/${Role.byId(roleId)}=" + (1.0 - accuracy))
    //
    //            //predictions.show()
    //            //df.show()
    //        })
    //
    //    }

    private def getNamesAndLoLScore(ds: Dataset[BoostedSummoner]): Unit = {
        ds.foreachPartition(partitionOfRecords => {

            Champions.populateMapIfEmpty()

            //Run over all the records and get the summonerId field of each one
            val summonerIds = new ListBuffer[SummonerId]
            partitionOfRecords.foreach(row => summonerIds += SummonerId(row.summonerId, Region.valueOf(row.region)))

            //Find names and scores that we don't know yet
            val unknownNames = new ListBuffer[SummonerId]
            val unknownScores = new ListBuffer[SummonerId]

            summonerIds.foreach(id => {
                RedisStore.getSummonerName(id).getOrElse(unknownNames += id)
                RedisStore.getSummonerLOLScore(id).getOrElse(unknownScores += id)
            })

            //group by regions and call their apis
            //TODO: Execute this in parallel for each region and call redis store async
            unknownNames.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                import collection.JavaConverters._
                api.getSummonerNamesByIds(ids.map(_.id).map(Long.box): _*).asScala.foreach(
                    mapping => RedisStore.addSummonerName(SummonerId(mapping._1, region), mapping._2))
            })

            unknownScores.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)

                api.getLeagueEntries(ids.map(_.id).map(Long.box): _*).asScala.foreach(
                    mapping => {
                        val lolScore = LoLScore(mapping._2.tier, mapping._2.division, mapping._2.leaguePoints)
                        RedisStore.addSummonerLOLScore(SummonerId(mapping._1, region), lolScore)
                    })
            })

        })
    }


}
