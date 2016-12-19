package gg.boosted.analyzers

import com.datastax.spark.connector._
import gg.boosted.dal.RedisStore
import gg.boosted.maps.Champions
import gg.boosted.posos.{LoLScore, SummonerId, SummonerMatch}
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.{Application, Role}
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


case class BoostedSummoner(
                          champion:String,
                          role:String,
                          name:String,
                          region:String,
                          winrate:Double,
                          rank:Int,
                          summonerId:Long,
                          tier:String,
                          division:String,
                          leaguePoints:Int,
                          lolScore:Int,
                          gamesPlayed:Int,
                          matches:Seq[Long]
                          )

/**
  * Created by ilan on 8/26/16.
  */
object BoostedSummoner {

    /**
      *
      * Calculate the Most boosted summoners at each role
      *
      * The calculation should speak for itself, but let's face it, it's kinda hard to read so...
      *
      * The inner subquery
      *
      * @param ds of type [SummonerMatch]
      * @param minGamesPlayed
      * @param since take into account only games played since that time
      * @return
      */
    def process(ds: Dataset[SummonerMatch], minGamesPlayed:Int, since:Long, maxRank:Int):Unit = {
        val boostedSummoners = findBoostedSummoners(ds, minGamesPlayed, since, maxRank).cache()
        //At this point i am at a fix. I need to get the summoner names and lolscore for all summoners that have gotten to this point.
        //The riot api allows me to get names and league entrries for multiple IDs and i need to do it in order to minimize the calls
        //To the riot api. However, i don't think there's a way to call a function for an entire column so i have to use a trick here...
        getNamesAndLoLScore(boostedSummoners)
        import Application.session.implicits._
        boostedSummoners.map(
            r=> {
                val summonerId = SummonerId(r._3, Region.valueOf(r._4))
                val summonerName = RedisStore.getSummonerName(summonerId)
                val lolScore = RedisStore.getSummonerLOLScore(summonerId).getOrElse(LoLScore("UNKNOWN","U",0))
                val champion = Champions.byId(r._1)
                val role = r._2
                val winrate = r._6
                val rank = r._8
                val gamesPlayed = r._5
                val matches = r._7
                //val lastUpdated = new Date()
                (champion, role, summonerName, summonerId.region.toString, winrate, rank, summonerId.id,
                    lolScore.tier, lolScore.division, lolScore.leaguePoints, lolScore.lolScore, gamesPlayed, matches)
            }).rdd.saveToCassandra("boostedgg", "boosted_summoners",
            SomeColumns("champion", "role", "summoner_name", "region", "winrate", "rank", "summoner_id",
                "tier", "division", "league_points", "lol_score", "games_played", "matches"))
    }

    def findBoostedSummoners(ds: Dataset[SummonerMatch], minGamesPlayed:Int, since:Long, maxRank:Int)
        :Dataset[(Int, String, Long, String, Long, Double, Seq[Long], Int)] = {

        //Use "distinct" so that in case a match got in more than once it will count just once
        import Application.session.implicits._
        ds.distinct().createOrReplaceTempView("MostBoostedSummoners") ;
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
          .map(r=>(r.getInt(0), Role.byId(r.getInt(1)).toString, r.getLong(2),
          r.getString(3), r.getLong(4), r.getDouble(5), r.getSeq[Long](6), r.getInt(7)))
    }

    private def getNamesAndLoLScore(ds:Dataset[(Int, String, Long, String, Long, Double, Seq[Long], Int)]):Unit = {
        ds.foreachPartition(partitionOfRecords => {

            Champions.populateMapIfEmpty()

            //Run over all the records and get the summonerId field of each one
            val summonerIds = new ListBuffer[SummonerId]
            partitionOfRecords.foreach(row => summonerIds += SummonerId(row._3, Region.valueOf(row._4)))

            //Find names and scores that we don't know yet
            val unknownNames = new ListBuffer[SummonerId]
            val unknownScores = new ListBuffer[SummonerId]

            summonerIds.foreach(id => {
                RedisStore.getSummonerName(id).getOrElse(unknownNames += id)
                RedisStore.getSummonerLOLScore(id).getOrElse(unknownScores += id)
            })

            //group by regions and call their apis
            //TODO: Execute this in parallel
            unknownNames.groupBy(_.region).foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                import collection.JavaConverters._
                api.getSummonerNamesByIds(ids.map(_.id).map(Long.box):_*).asScala.foreach(
                    mapping => RedisStore.addSummonerName(SummonerId(mapping._1, region), mapping._2))
            })

            unknownScores.groupBy(_.region).foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)

                api.getLeagueEntries(ids.map(_.id).map(Long.box):_*).asScala.foreach(
                    mapping => {
                        val lolScore = LoLScore(mapping._2.tier, mapping._2.division, mapping._2.leaguePoints)
                        RedisStore.addSummonerLOLScore(SummonerId(mapping._1, region), lolScore)
                    })
            })

        })
    }


}
