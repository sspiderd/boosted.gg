package gg.boosted

import gg.boosted.configuration.Configuration
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

  val checkPointDir = "/tmp/kuku3"

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
    val ssc = new StreamingContext(session.sparkContext, Seconds(Configuration.getLong("spark.kafka.polling")))

    val stream = Utilities.getKafkaSparkContext(ssc).window(Seconds(Configuration.getLong("window.size.seconds"))).map(value => SummonerMatch(value._2))
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
