package gg.boosted

import gg.boosted.posos.SummonerMatch
import gg.boosted.services.AnalyzerService
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * The entry point for this spark analyzer
  *
  * Created by ilan on 12/8/16.
  */
object Application {

  val checkPointDir = "/tmp/kuku1"

  private val master = "local[*]"
  private val appName = "boostedGG"

  val session:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()


  def context():StreamingContext = {
    val ssc = new StreamingContext(session.sparkContext, Seconds(30))

    val stream = Utilities.getKafkaSparkContext(ssc).window(Seconds(600000)).map(value => SummonerMatch(value._2))
    AnalyzerService.analyze(stream)

    ssc.checkpoint(checkPointDir)
    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate(checkPointDir, context _)

    ssc.start()
    ssc.awaitTermination()
  }

}
