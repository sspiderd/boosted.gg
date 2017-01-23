package gg.boosted.dal

import gg.boosted.posos.{SummonerMatchId, SummonerMatchSummary}
import gg.boosted.riotapi.{Region, RiotApi}
import gg.boosted.utils.JsonUtil
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 1/15/17.
  */
object SummonerMatches {

    val log = LoggerFactory.getLogger(SummonerMatches.getClass)

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

            //Get all matches
            //First group by region
            //then group by matches
            //then for each summoner create a SummonerMatchSummary and put in the redis
            unknownMatches.groupBy(_.region).par.foreach(regionGroup => {

                if (log.isDebugEnabled) {
                    val matches = regionGroup._2.groupBy(_.matchId)
                    val summoners = matches.map(_._2.map(_.summonerId))
                    log.debug(s"Retrieving ${matches.size} matches for ${summoners.size} summoners in '${regionGroup._1.toString}'")
                }

                regionGroup._2.groupBy(_.matchId).foreach(tuple => {
                    val matchId = tuple._1
                    val summonerIds = tuple._2.map(_.summonerId)
                    val region = regionGroup._1
                    val api = new RiotApi(region)
                    val fullMatch = api.getMatch(matchId, true)

                    summonerIds.foreach(summoner => {
                        val sms = SummonerMatchSummary(summoner, fullMatch)
                        RedisStore.addSummonerMatch(summoner, matchId, region.toString, JsonUtil.toJson(sms))
                    })
                })
            })
        })
    }

    def get(summonerId:Long, _matchId: Long, region: String): SummonerMatchSummary  =
        get(SummonerMatchId(summonerId, _matchId, Region.valueOf(region)))

    def get(summonerMatch: SummonerMatchId): SummonerMatchSummary = {
        RedisStore.getSummonerMatch(summonerMatch) match {
            case Some(x) => JsonUtil.fromJson[SummonerMatchSummary](x)
            case None => throw new RuntimeException(s"The match ${summonerMatch.matchId} for summoner ${summonerMatch.summonerId} at ${summonerMatch.region.toString} is unaccounted for. did you populate first?")
        }
    }


}
