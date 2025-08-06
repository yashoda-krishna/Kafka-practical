package com.example.kafkaclient;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerService {

    private static final String TOPIC = "my-topic"; // Replace with your topic name
    private static final String BOOTSTRAP_SERVERS = "10.0.7.144:9092";

    public void consumeFromPartition(int partition) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "partition-consumer-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(TOPIC, partition);
            consumer.assign(Collections.singletonList(topicPartition));
            System.out.println("🔁 Consuming messages from partition " + partition + " (Ctrl+C to stop)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("📥 Partition %d | Offset %d | Message: %s%n",
                            record.partition(), record.offset(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Consumer error: " + e.getMessage());
        }
    }
}
