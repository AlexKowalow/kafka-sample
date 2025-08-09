package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;

public class SampleConsumer {
    private static final Logger log = LoggerFactory.getLogger(SampleConsumer.class);

    public static void main(String[] args) {
        String ip = "192.168.50.63";
        Properties props = new Properties();
        props.put("bootstrap.servers", String.format("%s:9092, %s:9093, %s:9094", ip, ip, ip));
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);

        String[] topics = {"test"};

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topics));

        LocalDateTime finish = LocalDateTime.now().plusSeconds(60);
        while (LocalDateTime.now().isBefore(finish)) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("offset = {}, key = {}, value = {}, partition = {}", record.offset(), record.key(), record.value(), record.partition());
            }
        }

        consumer.close();
    }
}
