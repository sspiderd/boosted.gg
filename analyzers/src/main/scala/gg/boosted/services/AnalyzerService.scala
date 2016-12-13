package gg.boosted.services

import java.util.Date

import gg.boosted.Application
import gg.boosted.analyzers.{BoostedSummoner, BoostedSummoner$}
import gg.boosted.dal.{BoostedEntity, BoostedRepository}
import gg.boosted.maps.Summoners
import gg.boosted.posos.{SummonerId, SummonerMatch}
import gg.boosted.riotapi.Region
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

    val minGamesPlayed = 3

    def analyze(stream:DStream[SummonerMatch]):Unit = {

        //At this point i'm not sure why i need to work with DStreams at all so:
        stream.foreachRDD(rdd => {
            log.info("Processing at: " + new Date()) ;
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
        val topSummoners = BoostedSummoner.calculate(ds, minGamesPlayed, 0, maxRank).collect()

        log.info(s"Retrieved total of ${topSummoners.length}")

        val summonerIds = topSummoners.map ( s => SummonerId(s.summonerId.toLong, Region.valueOf(s.region)))

        //Get the names and scores of the output so we can store in the db
        val names = Summoners.getNames(summonerIds)

        val scores = Summoners.getLOLScores(summonerIds)

        val topSummonersEntities = topSummoners.map(summoner => {
            val id = SummonerId(summoner.summonerId.toLong, Region.valueOf(summoner.region))
            val name = names(id)
            val score = scores(id)
            BoostedEntity.toEntity(summoner, name, score)
        })

        if (topSummoners.length > 0) {
            BoostedRepository.insertMatches(topSummonersEntities, new Date())
        }
    }

    def convertToDataSet(rdd:RDD[SummonerMatch]):Dataset[SummonerMatch] = {
        import Application.session.implicits._
        return Application.session.createDataset(rdd)
    }

}
