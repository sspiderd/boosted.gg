package gg.boosted.analyzers

import java.util.Date

import com.datastax.spark.connector._
import gg.boosted.configuration.Configuration
import gg.boosted.dal.RedisStore
import gg.boosted.maps.{Champions, Items}
import gg.boosted.posos._
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
        val matchedEvents = boostedSummonersToWeightedMatchSummary(boostedSummoners)

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


    def boostedSummonersToWeightedMatchSummary(ds: Dataset[BoostedSummoner])
    :DataFrame = {

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
        }).toDF("matchId", "summonerId", "region", "championId", "roleId", "winner", "coreItems")
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
        val summonerMatchSummaryWithWeights = summonerMatchSummary.map(row => {
            val coreItems = row.getSeq[Int](6)
            val aw = coreItems.map(Items.byId).map(Items.weights).reduce(Items.accumulatedWeight)
            (row.getLong(0), row.getLong(1), row.getString(2), row.getInt(3), row.getInt(4), row.getInt(5), coreItems, row.getLong(7),
            aw.attackDamage, aw.abilityPower, aw.armor, aw.magicResistance, aw.health, aw.mana, aw.healthRegen, aw.manaRegen,
            aw.criticalStrikeChance, aw.attackSpeed, aw.flatMovementSpeed, aw.lifeSteal, aw.percentMovementSpeed)
        }).toDF("matchId", "summonerId", "region", "championId", "roleId", "winner", "coreItems", "gamesPlayedWithCoreItems",
        "ad", "ap", "armor", "mr", "health", "mana", "healthRegen", "manaRegen", "criticalStrikeChance", "as", "flatMS", "lifeSteal",
        "percentMS")
        summonerMatchSummaryWithWeights
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

        println(s"Calculating for champion ${Champions.byId(championId)} and role ${Role.byId(roleId)}")
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
