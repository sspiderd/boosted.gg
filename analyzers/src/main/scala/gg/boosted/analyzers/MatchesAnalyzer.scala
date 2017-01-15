package gg.boosted.analyzers

import gg.boosted.dal.Matches
import gg.boosted.posos.{BoostedSummoner, SummonerMatchSummary}
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 1/15/17.
  */
object MatchesAnalyzer {

  val log = LoggerFactory.getLogger(MatchesAnalyzer.getClass)

  def boostedSummonersToSummaries(boostedSummoners:Dataset[BoostedSummoner]):Dataset[SummonerMatchSummary] = {
    Matches.populateMatches(boostedSummoners.toDF())
    return null
  }

}
