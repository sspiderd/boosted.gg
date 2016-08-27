package gg.boosted.services

import gg.boosted.{Spark, Utilities}
import gg.boosted.analyzers.{BoostedSummonersChrolesToWR, DataFrameUtils}
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
            if (rdd != null && rdd.count() > 0) {
                println("------")
                val df = Utilities.smRDDToDF(rdd) ;

                val calced = BoostedSummonersChrolesToWR.calc(df, 1, 0).cache()

                val chroles = DataFrameUtils.findDistinctChampionAndRoleIds(calced) ;

                println (chroles)

                chroles.foreach(chrole => {
                    val chroleDf = BoostedSummonersChrolesToWR.filterByChrole(calced, chrole.championId, chrole.roleId)
                    if (chroleDf.count() > 0) {
                        println(s"-- $chrole --")
                        chroleDf.show()
                    }
                })
            }
        })
    }

    def functionToCreateContext():StreamingContext = {
        val ssc = Spark.ssc
        val stream = Utilities.getKafkaSparkContext(ssc).window(Seconds(180), Seconds(4)).map(value => SummonerMatch(value._2))
        boostedSummonersToChrole(stream)
        ssc.checkpoint("/tmp/kuku")
        ssc
    }

    def main(args: Array[String]): Unit = {

        //val stream = Utilities.getKafkaSparkContext(Spark.ssc).window(Seconds(180), Seconds(4)).map(value => SummonerMatch(value._2))

        val ssc = StreamingContext.getOrCreate("/tmp/kuku", functionToCreateContext _)

        ssc.start()
        ssc.awaitTermination()
    }

}
