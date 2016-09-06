package gg.boosted.analyzers

import org.apache.spark.sql.{DataFrame, Row}

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
      * @param df of type [SummonerMatch]
      * @param gamesPlayed
      * @param since
      * @return df of type [SummonerChrole]
      */
    def calc(df: DataFrame, gamesPlayed:Int, since:Long, maxRank:Int):DataFrame = {
        //Use "distinct" so that in case a match got in more than once it will count just once
        df.distinct().createOrReplaceTempView("BoostedSummonersChrolesToWR_calc") ;
        df.sparkSession.sql(
            s"""SELECT championId, roleId, summonerId, region, gamesPlayed, winrate, matches,
               |rank() OVER (PARTITION BY championId, roleId ORDER BY winrate DESC, gamesPlayed DESC, summonerId DESC) as rank FROM (
               |SELECT championId, roleId, summonerId, region, count(*) as gamesPlayed, (sum(if (winner=true,1,0))/count(winner)) as winrate, collect_list(matchId) as matches
               |FROM BoostedSummonersChrolesToWR_calc
               |WHERE date >= $since
               |GROUP BY championId, roleId, summonerId, region
               |HAVING winrate > 0.5 AND gamesPlayed >= $gamesPlayed
               |) having rank <= $maxRank
      """.stripMargin)
    }

    /**
      * The basic idea is to run the above "calc" method, cache it, and then run it through this method
      * for each champion and role
      *
      * @param df of type [SummonerChrole]
      * @param championId
      * @param roleId
      * @return df of type [SummonerChrole], but filtered
      */
    def filterByChrole(df: DataFrame, championId:Int, roleId: Int):DataFrame = {
        df.filter(s"championId = $championId and roleId = $roleId")
    }

    def apply(row: Row):BoostedSummonersChrolesToWR = {
        BoostedSummonersChrolesToWR(
            row.getInt(0),
            row.getInt(1),
            row.getLong(2),
            row.getString(3),
            row.getLong(4),
            row.getDouble(5),
            row.getSeq[Long](6),
            row.getInt(7)
        )
    }

}
