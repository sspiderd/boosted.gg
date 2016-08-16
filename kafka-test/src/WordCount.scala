import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ilan on 8/15/16.
  */
object WordCount {

  def wordCount(str: RDD[String]):RDD[(String, Int)] = {
    return str.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y)
  }

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "MostBoostedChampionAtRole", Seconds(1))

    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val stream = ssc.socketTextStream("localhost", 9000)
    stream.window(Seconds(5), Seconds(1)).transform(wordCount(_)).foreachRDD(rdd => {
      rdd.collect().foreach(println (_))
      println ("------")
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
