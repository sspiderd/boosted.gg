package gg.boosted.services

import java.util.Date

import gg.boosted.analyzers.{BoostedSummonersChrolesToWR, DataFrameUtils}
import gg.boosted.dal.{BoostedEntity, BoostedRepository}
import gg.boosted.posos.SummonerMatch
import gg.boosted.utils.SummonerIdToName
import gg.boosted.{Spark, Utilities}
import net.rithms.riot.api.{ApiConfig, RiotApi}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/26/16.
  */
object AnalyzerService {

    val log = LoggerFactory.getLogger(AnalyzerService.getClass)

    val checkPointDir = "/tmp/kuku1"

    val maxRank = 30

    val gamesPlayed = 4

    def boostedSummonersToChrole(stream:DStream[SummonerMatch]):Unit = {
        stream.foreachRDD(rdd => {
            log.debug("Processing at: " + new Date())
            if (rdd != null && rdd.count() > 0) {

                val df = Utilities.smRDDToDF(rdd) ;

                //Get the boosted summoner by champion and role
                val calced = BoostedSummonersChrolesToWR.calc(df, gamesPlayed, 0, maxRank)

                //Map it boosted entity
                val topSummoners = calced
                    .collect()
                    .map(BoostedSummonersChrolesToWR(_))

                SummonerIdToName.populateSummonerNamesByIds(topSummoners.groupBy(_.region).mapValues(_.map(_.summonerId)))

                val topSummonersEntities = topSummoners.map(BoostedEntity(_))

                if (topSummoners.length > 0) {
                    BoostedRepository.insertMatches(topSummonersEntities, new Date())
                }


            } else {
                log.debug("RDD not found")
            }
        })
    }

    def context():StreamingContext = {
        val ssc = new StreamingContext(Spark.session.sparkContext, Seconds(30))

        val stream = Utilities.getKafkaSparkContext(ssc).window(Seconds(600000)).map(value => SummonerMatch(value._2))
        boostedSummonersToChrole(stream)

        ssc.checkpoint(checkPointDir)
        ssc
    }

    def main(args: Array[String]): Unit = {

        val ssc = StreamingContext.getOrCreate(checkPointDir, context _)

        ssc.start()
        ssc.awaitTermination()
    }

}
