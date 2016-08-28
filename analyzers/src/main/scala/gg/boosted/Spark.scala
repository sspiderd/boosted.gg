package gg.boosted

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ilan on 8/27/16.
  */
object Spark {

    private val master = "local[*]"
    private val appName = "boostedGG"

    val session:SparkSession = SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .getOrCreate()

}
