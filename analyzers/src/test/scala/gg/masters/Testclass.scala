package gg.masters

import java.nio.file.Files

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ilan on 8/15/16.
  */
class Testclass extends FlatSpec with BeforeAndAfter with Eventually {

    private val master = "local[*]"
    private val appName = "example-spark-streaming"
    private val batchDuration = Seconds(1)
    private val checkpointDir = Files.createTempDirectory(appName).toString

    private var sc: SparkContext = _
    private var ssc: StreamingContext = _

    before {
        val conf = new SparkConf()
                .setMaster(master)
                .setAppName(appName)

        ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint(checkpointDir)

        sc = ssc.sparkContext
    }

    after {
        if (ssc != null) {
            ssc.stop()
        }
    }

    "Some bullshit" should "do some bullshit" in {

//        val summonerGames = Seq[SummonerGame] (
//            SummonerGame(1, 1, 1, "TOP", true),
//            SummonerGame(1, 2, 2, "MIDDLE", true)
//        )
//
//        val summonerGame2 = Seq[SummonerGame] (
//            SummonerGame(2, 1, 1, "TOP", true),
//            SummonerGame(2, 2, 2, "MIDDLE", false)
//        )
//
//        //setupLogging()
//
//        val rdd1 = sc.parallelize(summonerGames)
//        val rdd2 = sc.parallelize(summonerGame2)
//
//        val q = scala.collection.mutable.Queue[RDD[SummonerGame]] (rdd1, rdd2)
//
//
//        val stream = ssc.queueStream(q, true) ;
//
//        val result = MostBoostedChampionAtRole.summonerChampionRoleToWinrate(stream)
//        result.foreachRDD(rdd => print(rdd.collect().last))
//        eventually {
//            result.foreachRDD(rdd => rdd.collect().last == ((2,2,"MIDDLE"), 0.5))
//        }
//
//        ssc.start()
//        ssc.awaitTermination()
    }

}
