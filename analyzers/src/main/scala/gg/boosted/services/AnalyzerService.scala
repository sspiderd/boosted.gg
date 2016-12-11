package gg.boosted.services

import java.util.Date

import gg.boosted.Application
import gg.boosted.analyzers.MostBoostedSummoners
import gg.boosted.dal.{BoostedEntity, BoostedRepository}
import gg.boosted.maps.{SummonerIdToLoLScore, SummonerIdToName, Summoners}
import gg.boosted.posos.SummonerMatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/26/16.
  */
object AnalyzerService {

    val log = LoggerFactory.getLogger(AnalyzerService.getClass)

    val maxRank = 1000

    val minGamesPlayed = 1

    def analyze(stream:DStream[SummonerMatch]):Unit = {

        //At this point i'm not sure why i need to work with DStreams at all so:
        stream.foreachRDD(rdd => {
            log.debug("Processing at: " + new Date()) ;
            analyze(rdd)
        })
    }

    def analyze(rdd:RDD[SummonerMatch]):Unit = {
        //Convert the rdd to ds so we can use Spark SQL on it
        if (rdd != null && rdd.count() > 0) {
            analyze(convertToDataSet(rdd))
        } else {
            log.debug("RDD is empty")
        }

    }

    def analyze(ds:Dataset[SummonerMatch]):Unit = {
        //Get the boosted summoner DF by champion and role
        val topSummoners = MostBoostedSummoners.calculate(ds, minGamesPlayed, 0, maxRank).collect()

        //I don't know whether different regions can share summoner ids or not, since that is the case
        //I'm assuming that the answer is "no" and so i need to keep a map of region->summonerIds
        val regionToSummonerIds = topSummoners.groupBy(_.region).mapValues(_.map(_.summonerId))

        //regionToSummonerIds.foreach(k => Summoners.getNames(k._2, k._1))

        //SummonerIdToName.populateSummonerNamesByIds(regionToSummonerIds)
        //SummonerIdToLoLScore.populateLoLScoresByIds(regionToSummonerIds)

        val topSummonersEntities = topSummoners.map(BoostedEntity(_))

        if (topSummoners.length > 0) {
            BoostedRepository.insertMatches(topSummonersEntities, new Date())
        }
    }

    def convertToDataSet(rdd:RDD[SummonerMatch]):Dataset[SummonerMatch] = {
        import Application.session.implicits._
        return Application.session.createDataset(rdd)
    }

}
