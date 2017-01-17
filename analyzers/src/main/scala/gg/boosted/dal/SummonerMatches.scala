package gg.boosted.dal

import gg.boosted.posos.{SummonerMatchId, SummonerMatchSummary}
import gg.boosted.riotapi.dtos.{MatchSummary, SummonerMatchDetails}
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.utils.JsonUtil
import org.apache.spark.sql.DataFrame

/**
  * Created by ilan on 1/15/17.
  */
object SummonerMatches {

    /**
      * Send me a dataframe that has columns: "summonerId", "matches", "region", each row should contain a list of matches
      *
      * @param matches
      *
      */
    def populateSummonerMatches(matches: DataFrame): Unit = {

        val _matches = matches.select("summonerId", "matches", "region")

        //Download the matches from the dataframe
        _matches.foreachPartition(partitionOfRecords => {
            var allSummonerMatchIds = Set[SummonerMatchId]()

            partitionOfRecords.foreach(row => {
                val summonerId = row.getLong(0)
                val _matches = row.getSeq[Long](1)
                val _region = row.getString(2)
                _matches.foreach(
                    _match => allSummonerMatchIds += SummonerMatchId(summonerId, _match, Region.valueOf(_region)))
            })

            //Get all unknown matches
            var unknownMatches = Set[SummonerMatchId]()
            allSummonerMatchIds.foreach(id => RedisStore.getSummonerMatch(id.summonerId, id.matchId, id.region.toString).getOrElse(unknownMatches += id))

            unknownMatches.groupBy(_.region).par.foreach(tuple => {
                val region = tuple._1
                val ids = tuple._2
                val api = new RiotApi(region)
                ids.foreach(id => RedisStore.addSummonerMatch(id.summonerId, id.matchId, region.toString, api.getSummonerMatchDetailsAsJson(id.summonerId, id.matchId)))
            })
        })
    }

    def matchesToSummonerMatches(matches:Set[])

    def get(summonerId:Long, _matchId: Long, region: String): SummonerMatchDetails  =
        get(SummonerMatchId(summonerId, _matchId, Region.valueOf(region)))

    def get(summonerMatch: SummonerMatchId): SummonerMatchDetails = {
        RedisStore.getSummonerMatch(summonerMatch) match {
            case Some(x) => JsonUtil.fromJson[SummonerMatchDetails](x)
            case None => throw new RuntimeException(s"The match ${summonerMatch.matchId} for summoner ${summonerMatch.summonerId} at ${summonerMatch.region.toString} is unaccounted for. did you populate first?")
        }
    }


}
