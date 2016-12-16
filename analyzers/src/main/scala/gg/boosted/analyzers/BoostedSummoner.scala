package gg.boosted.analyzers

import gg.boosted.maps.Champions
import gg.boosted.posos.SummonerMatch
import gg.boosted.{Application, Role}
import org.apache.spark.sql.Dataset

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
        val v = ds.sparkSession.sql(
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
                                    r.getString(3), r.getLong(4), r.getDouble(5), r.getSeq[Long](6), r.getInt(7)))//.as[BoostedSummoner]
        //At this point i am at a fix. I need to get the summoner names and lolscore for all summoners that have gotten to this point.
        //The riot api allows me to get names and league entrries for multiple IDs and i need to do it in order to minimize the calls
        //To the riot api. However, i don't think there's a way to call a function for an entire column so i have to use a trick here...
            .map(r=>(r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, LazyLoader.loadNameLazy(r._3, r._4)))
            .map(r=>(r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, LazyLoader.getName(r._3, r._4)))
        v.collect()
        return null
    }


}
