package gg.boosted.services

import java.util.Date

import gg.boosted.{Spark, Utilities}
import gg.boosted.analyzers.{BoostedSummonersChrolesToWR, DataFrameUtils}
import gg.boosted.dal.BoostedRepository
import gg.boosted.posos.SummonerMatch
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ilan on 8/26/16.
  */
object AnalyzerService {

    val checkPointDir = "/tmp/kuku5"

    val topXSummoners = 30

    def boostedSummonersToChrole(stream:DStream[SummonerMatch]):Unit = {
        stream.foreachRDD(rdd => {
            println ("I AM HERE: " + new Date())
            if (rdd != null && rdd.count() > 0) {
                println("------")
                val df = Utilities.smRDDToDF(rdd) ;

                val calced = BoostedSummonersChrolesToWR.calc(df, 1, 0).cache()

                val chroles = DataFrameUtils.findDistinctChampionAndRoleIds(calced) ;

                chroles.foreach(chrole => {
                    val chroleDf = BoostedSummonersChrolesToWR.filterByChrole(calced, chrole.championId, chrole.roleId)
                    if (chroleDf.count() > 0) {
                        println(s"-- $chrole --")
                        val topSummonersForChroles = chroleDf.take(topXSummoners).map(Utilities.rowToSummonerChrole)
                        BoostedRepository.insertMatches(topSummonersForChroles)
                        chroleDf.show()
                    }
                })
            }
        })
    }

    def context():StreamingContext = {
        val ssc = new StreamingContext(Spark.session.sparkContext, Seconds(5))

        val stream = Utilities.getKafkaSparkContext(ssc).window(Seconds(180)).map(value => SummonerMatch(value._2))
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
