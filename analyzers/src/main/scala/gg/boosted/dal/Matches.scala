package gg.boosted.dal

import gg.boosted.posos.MatchId
import gg.boosted.riotapi.dtos.MatchSummary
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.utils.JsonUtil
import org.apache.spark.sql.DataFrame

/**
  * Created by ilan on 1/15/17.
  */
object Matches {

    /**
      * Send me a dataframe that has columns: "matches", "region", each row should contain a list of matches
      *
      * @param matches
      *
      */
    def populateMatches(matches: DataFrame): Unit = {

        val _matches = matches.select("matches", "region")

        //Download the matches from the dataframe
        _matches.foreachPartition(partitionOfRecords => {
            var allMatchIds = Set[MatchId]()

            partitionOfRecords.foreach(row => {
                val _matches = row.getSeq[Long](0)
                val _region = row.getString(1)
                _matches.foreach(
                    _match => allMatchIds += MatchId(_match, Region.valueOf(_region)))
            })

            //Get all unknown matches
            var unknownMatches = Set[MatchId]()
            allMatchIds.foreach(id => RedisStore.getMatch(id).getOrElse(unknownMatches += id))

            unknownMatches.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                ids.foreach(id => RedisStore.addMatch(id, api.getMatchSummaryAsJson(id.id)))
            })
        })
    }

    def get(_matchId: Long, region: String): MatchSummary = {
        get(MatchId(_matchId, Region.valueOf(region)))
    }

    def get(_match: MatchId): MatchSummary = {
        RedisStore.getMatch(_match) match {
            case Some(x) => JsonUtil.fromJson[MatchSummary](x)
            case None => throw new RuntimeException(s"The match ${_match.id} at ${_match.region.toString} is unaccounted for. did you populate first?")
        }
    }


}
