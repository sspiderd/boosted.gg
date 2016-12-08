package gg.boosted.analyzers

import gg.boosted.Application
import gg.boosted.posos.SummonerMatch
import org.apache.spark.sql.{DataFrame, Dataset, Row}

case class BoostedSummonersChrolesToWR(
                                         championId: Int,
                                         roleId: Int,
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
object BoostedSummonersChrolesToWR {

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
      * @param since
      * @return df of type [SummonerChrole]
      */
    def calc(ds: Dataset[SummonerMatch], minGamesPlayed:Int, since:Long, maxRank:Int):Dataset[BoostedSummonersChrolesToWR] = {
        //Use "distinct" so that in case a match got in more than once it will count just once
        import Application.session.implicits._
        ds.distinct().createOrReplaceTempView("BoostedSummonersChrolesToWR_calc") ;
        ds.sparkSession.sql(
            s"""SELECT championId, roleId, summonerId, region, gamesPlayed, winrate, matches,
               |rank() OVER (PARTITION BY championId, roleId ORDER BY winrate DESC, gamesPlayed DESC, summonerId DESC) as rank FROM (
               |SELECT championId, roleId, summonerId, region, count(*) as gamesPlayed, (sum(if (winner=true,1,0))/count(winner)) as winrate, collect_list(matchId) as matches
               |FROM BoostedSummonersChrolesToWR_calc
               |WHERE date >= $since
               |GROUP BY championId, roleId, summonerId, region
               |HAVING winrate > 0.5 AND gamesPlayed >= $minGamesPlayed
               |) having rank <= $maxRank
      """.stripMargin).as[BoostedSummonersChrolesToWR]
    }


}
