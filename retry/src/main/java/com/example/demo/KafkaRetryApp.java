package com.example.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaRetryApp {

    private static final String TOPIC_NAME = "my-topic";
    private static final int MAX_RETRIES = 5;
    private static final long RETRY_INTERVAL_MS = 2000;
    private static final String BOOTSTRAP_SERVERS = "local-host:9092";

    public static void main(String[] args) {

        if (!isKafkaAvailable(BOOTSTRAP_SERVERS)) {
            System.err.println("❌ Kafka is not available. Exiting.");
            System.exit(1);
        }

        // Retry Kafka Consumer
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try (KafkaConsumer<String, String> consumer = createConsumer()) {
                consumer.subscribe(Collections.singletonList(TOPIC_NAME));
                System.out.println("✅ Consum"
                		+ ""
                		+ ""
                		+ ""
                		+ ""
                		+ "er connected and subscribed to topic: " + TOPIC_NAME);
                break;
            } catch (Exception e) {
                handleRetry("Consumer", i, e);
            }
        }

        // Retry Kafka Producer
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "Hello Kafka");
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("✅ Producer sent message to topic " + metadata.topic() + " at offset " + metadata.offset());
                break;
            } catch (Exception e) {
                handleRetry("Producer", i, e);
            }
        }
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "retry-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 0); // Manual retry logic
        return new KafkaProducer<>(props);
    }

    private static boolean isKafkaAvailable(String bootstrapServers) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            DescribeClusterResult result = admin.describeCluster();
            String clusterId = result.clusterId().get(5, TimeUnit.SECONDS);
            System.out.println("✅ Kafka is available. Cluster ID: " + clusterId);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Kafka health check failed: " + e.getMessage());
            return false;
        }
    }

    private static void handleRetry(String type, int attempt, Exception e) {
        System.err.printf("⚠️ %s retry %d/%d failed: %s%n", type, attempt, MAX_RETRIES, e.getMessage());
        if (attempt == MAX_RETRIES) {
            System.err.printf("❌ Max retries reached for %s. Exiting.%n", type);
            System.exit(1);
        }
        try {
            Thread.sleep(RETRY_INTERVAL_MS * attempt);
        } catch (InterruptedException ie) {
            System.err.printf("❌ %s retry interrupted. Exiting.%n", type);
            System.exit(1);
        }
    }
}
