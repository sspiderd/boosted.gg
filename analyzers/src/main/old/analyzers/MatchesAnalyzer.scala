package gg.boosted.analyzers

import gg.boosted.Role
import gg.boosted.configuration.Configuration
import gg.boosted.dal.{RedisStore, SummonerMatches}
import gg.boosted.maps.Items
import gg.boosted.posos.{BoostedSummoner, SummonerMatchSummary}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import gg.boosted.Application.session.implicits._
import gg.boosted.utils.JsonUtil

/**
  * Created by ilan on 1/15/17.
  */
object MatchesAnalyzer {

    val log = LoggerFactory.getLogger(MatchesAnalyzer.getClass)

    def boostedSummonersToSummaries(boostedSummoners: Dataset[BoostedSummoner]): Dataset[SummonerMatchSummary] = {
        SummonerMatches.populateSummonerMatches(boostedSummoners.toDF())

        boostedSummoners.flatMap(row => {

            val summonerId = row.summonerId
            val region = row.region

            row.matches.map(matchId =>
                JsonUtil.fromJson[SummonerMatchSummary](RedisStore.getSummonerMatch(summonerId, matchId, region.toString).
                    getOrElse(throw new RuntimeException("This can't happen"))))
        })
    }

}
