package gg.boosted

import groovy.transform.CompileStatic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Created by ilan on 8/11/16.
 */
@CompileStatic
class KafkaSummonerMatchProducer {

    static Producer<String, String> producer ;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.0.3:9092")
        props.put("acks", "all")
        props.put("retries", 0)
        props.put("batch.size", 16384)
        props.put("linger.ms", 1)
        props.put("buffer.memory", 33554432)
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

        producer = new KafkaProducer<>(props);
    }

    static send(SummonerMatch summonerMatch) {

        producer.send(new ProducerRecord<>("boostedgg", summonerMatch.summonerId, MessagePacker.pack(summonerMatch)))
        producer.flush()
    }

}
