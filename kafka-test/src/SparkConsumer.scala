/**
  * Created by ilan on 7/25/16.
  */
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka._
object SparkConsumer {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount");
        val ssc = new StreamingContext(sparkConf, Durations.seconds(2));

        // Create direct kafka stream with brokers and topics
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                ssc,  kafkaParams, topicsSet)

    }

}
