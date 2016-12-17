package gg.boosted.analyzers

import gg.boosted.dal.RedisStore
import gg.boosted.maps.{Champions, RiotApis}
import gg.boosted.posos.{LoLScore, SummonerId, SummonerMatch}
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.{Application, Role}
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

case class BoostedSummoner(
                                 champion: String,
                                 role: String,
                                 summonerId: Long,
                                 region: String,
                                 gamesPlayed: Long,
                                 winrate: Double,
                                 matches: Seq[Long],
                                 rank: Int
)

/**
  * Created by ilan on 8/26/16.
  */
object BoostedSummoner {


    val idToNameMap = scala.collection.mutable.HashMap.empty[SummonerId, String]

    //val idToLoLScoreMap = Map[Long, LoLScore]

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
      * @return ds of type [MostBoostedSummoners]
      */
    def calculate(ds: Dataset[SummonerMatch], minGamesPlayed:Int, since:Long, maxRank:Int):Dataset[BoostedSummoner] = {
        //Use "distinct" so that in case a match got in more than once it will count just once
        import Application.session.implicits._
        ds.distinct().createOrReplaceTempView("MostBoostedSummoners") ;
        val intermed = ds.sparkSession.sql(
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
            .map(r=>(Champions.byId(r.getInt(0)), Role.byId(r.getInt(1)).toString, r.getLong(2),
                                    r.getString(3), r.getLong(4), r.getDouble(5), r.getSeq[Long](6), r.getInt(7))).cache()//.as[BoostedSummoner]
        //At this point i am at a fix. I need to get the summoner names and lolscore for all summoners that have gotten to this point.
        //The riot api allows me to get names and league entrries for multiple IDs and i need to do it in order to minimize the calls
        //To the riot api. However, i don't think there's a way to call a function for an entire column so i have to use a trick here...
        getNamesAndLoLScore(intermed)
        val x = intermed.map(
            r=> {
                val summonerId = SummonerId(r._3, Region.valueOf(r._4))
                val summonerName = RedisStore.getSummonerName(summonerId)
                val lolScore = RedisStore.getSummonerLOLScore(summonerId).get
                (r._1, r._2, summonerName,
                    r._4, lolScore.tier, lolScore.division, lolScore.leaguePoints, lolScore.lolScore,
                    r._5, r._6, r._7, r._8)
            })
        val y = x
        return null
    }

    private def getNamesAndLoLScore(ds:Dataset[(String, String, Long, String, Long, Double, Seq[Long], Int)]):Unit = {
        ds.foreachPartition(partitionOfRecords => {
            //Run over all the records and get the summonerId field of each one
            val summonerIds = new ListBuffer[SummonerId]
            partitionOfRecords.foreach(row => summonerIds += SummonerId(row._3, Region.valueOf(row._4)))

            //group by regions and call their apis
            //TODO: throw Redis in the mix
            //TODO: Execute this in parallel
            summonerIds.groupBy(_.region).foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                import collection.JavaConverters._
                api.getSummonerNamesByIds(ids.map(_.id).map(Long.box):_*).asScala.foreach(
                    mapping => RedisStore.addSummonerName(SummonerId(mapping._1, region), mapping._2))
                api.getLeagueEntries(ids.map(_.id).map(Long.box):_*).asScala.foreach(
                    mapping => {
                        val lolScore = LoLScore(mapping._2.tier, mapping._2.division, mapping._2.leaguePoints)
                        RedisStore.addSummonerLOLScore(SummonerId(mapping._1, region), lolScore)
                    })
            })
        })
    }


}
