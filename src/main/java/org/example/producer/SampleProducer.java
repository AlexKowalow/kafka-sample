package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class SampleProducer {

    private static final Logger log = LoggerFactory.getLogger(SampleProducer.class);

    public static void main(String[] args) throws InterruptedException {
        String clientId = String.valueOf(UUID.randomUUID());
        String ip = "192.168.50.63";
        Properties props = new Properties();
        props.put("bootstrap.servers", String.format("%s:9092, %s:9093, %s:9094", ip, ip, ip));
        props.put("acks", "all");
        props.put("client.id", clientId);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        String topic = "test";

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            String message = String.format("Producer %s has sent message %s at %s", clientId, i, new Date());
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), message));
            log.info("Message: {}", message);
            Thread.sleep(150);
        }

        producer.close();
    }
}
