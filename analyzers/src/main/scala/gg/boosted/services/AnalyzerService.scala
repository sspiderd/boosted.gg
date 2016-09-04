package gg.boosted.services

import java.util.Date

import gg.boosted.analyzers.{BoostedSummonersChrolesToWR, DataFrameUtils}
import gg.boosted.dal.{BoostedEntity, BoostedRepository}
import gg.boosted.posos.SummonerMatch
import gg.boosted.{Spark, Utilities}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by ilan on 8/26/16.
  */
object AnalyzerService {

    val log = LoggerFactory.getLogger(AnalyzerService.getClass)

    val checkPointDir = "/tmp/kuku2"

    val topXSummoners = 30

    val gamesPlayed = 4

    def boostedSummonersToChrole(stream:DStream[SummonerMatch]):Unit = {
        stream.foreachRDD(rdd => {
            log.debug("Processing at: " + new Date())
            if (rdd != null && rdd.count() > 0) {
                val df = Utilities.smRDDToDF(rdd) ;

                val calced = BoostedSummonersChrolesToWR.calc(df, gamesPlayed, 0).cache()

                val chroles = DataFrameUtils.findDistinctChampionAndRoleIds(calced) ;

                BoostedRepository.truncateTable()

                chroles.foreach(chrole => {

                    log.debug(s"Running for ${chrole.championId} / ${chrole.roleId}")

                    val chroleDf = BoostedSummonersChrolesToWR.filterByChrole(calced, chrole.championId, chrole.roleId)

                    //chroleDf.show()
                    if (chroleDf.count() > 0) {
                        //println(s"-- $chrole --")
                        val topSummonersForChroles = chroleDf
                            .take(topXSummoners)
                            .map(row => BoostedSummonersChrolesToWR(row))
                            .map(BoostedEntity(_))
                        BoostedRepository.insertMatches(topSummonersForChroles)
                        //chroleDf.show()
                    }
                })
            } else {
                log.debug("RDD not found")
            }
        })
    }

    def context():StreamingContext = {
        val ssc = new StreamingContext(Spark.session.sparkContext, Seconds(30))

        val stream = Utilities.getKafkaSparkContext(ssc).window(Seconds(6000)).map(value => SummonerMatch(value._2))
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
