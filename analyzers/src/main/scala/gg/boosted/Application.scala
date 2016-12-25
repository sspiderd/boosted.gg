package gg.boosted

import gg.boosted.configuration.Configuration
import gg.boosted.posos.SummonerMatch
import gg.boosted.services.AnalyzerService
import gg.boosted.utils.KafkaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, Minutes, Seconds, StreamingContext}

/**
  *
  * The entry point for this spark analyzer
  *
  * Created by ilan on 12/8/16.
  */
object Application {

  val checkPointDir = "/tmp/kuku4"

  private val master = "local[*]"
  private val appName = Configuration.getString("kafka.topic")

  val session:SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
      .config("spark.cassandra.connection.host", Configuration.getString("cassandra.location"))
    .getOrCreate()


  def context():StreamingContext = {
    //Check every 30 seconds
    val ssc = new StreamingContext(session.sparkContext, Minutes(Configuration.getLong("calculate.every.n.minutes")))

    val stream = KafkaUtil.getKafkaSparkContext(ssc).window(Minutes(Configuration.getLong("window.size.minutes"))).map(value => SummonerMatch(value._2))
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
