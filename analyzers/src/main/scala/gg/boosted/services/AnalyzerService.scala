package gg.boosted.services

import gg.boosted.{Spark, Utilities}
import gg.boosted.analyzers.BoostedSummonersChrolesToWR
import gg.boosted.posos.SummonerMatch
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ilan on 8/26/16.
  */
object AnalyzerService {

    private val master = "local[*]"
    private val appName = "BoostedSummonersChrolesToWR"

    def boostedSummonersToChrole(stream:DStream[SummonerMatch]):Unit = {
        stream.foreachRDD(rdd => {
            println("------")
            val df = Utilities.smRDDToDF(rdd) ;

            val calced = BoostedSummonersChrolesToWR.calc(df, 1, 0).cache()

            for (championId <- 1 to 131) {
                for (roleId <- 1 to 5) {
                    val chroleDf = BoostedSummonersChrolesToWR.filterByChrole(calced, championId, roleId)
                    if (chroleDf.count() > 0) {
                        println(s"-- $championId/$roleId --")
                        chroleDf.show()
                    }
                }
            }
        })
    }

    def main(args: Array[String]): Unit = {

        val stream = Utilities.getKafkaSparkContext(Spark.ssc).window(Seconds(180), Seconds(10)).map(value => SummonerMatch(value._2))
        boostedSummonersToChrole(stream)

        Spark.ssc.start()
        Spark.ssc.awaitTermination()
    }

}
