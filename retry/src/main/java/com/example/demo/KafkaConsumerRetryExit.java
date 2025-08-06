package com.example.demo;

//
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.DescribeClusterResult;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//
//import java.util.Collections;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//public class KafkaConsumerRetryExit {
//
//    private static final String TOPIC_NAME = "my-topic";
//    private static final int MAX_RETRIES = 5;
//    private static final long RETRY_INTERVAL_MS = 2000;
//
//    public static void main(String[] args) {
//
//        String broker = "local-host:9092"; // Corrected 'local-host' to 'localhost'
//        Properties props = new Properties();
//        props.put("bootstrap.servers", broker);
//        props.put("group.id", "retry-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        if (!isKafkaAvailable(props)) {
//            System.err.println("❌ Kafka is not available. Exiting.");
//            System.exit(1);
//        }
//
//        for (int i = 1; i <= MAX_RETRIES; i++) {
//            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
//                consumer.subscribe(Collections.singletonList(TOPIC_NAME));
//                System.out.println("✅ Connected to Kafka and subscribed to topic: " + TOPIC_NAME);
//                return;
//            } catch (Exception e) {
//                System.err.printf("⚠️ Retry %d/%d failed: %s%n", i, MAX_RETRIES, e.getMessage());
//                if (i == MAX_RETRIES) {
//                    System.err.println("❌ Max retries reached. Exiting.");
//                    System.exit(1);
//                }
//                try {
//                    Thread.sleep(RETRY_INTERVAL_MS * i);
//                } catch (InterruptedException ie) {
//                    System.err.println("❌ Retry interrupted. Exiting.");
//                    System.exit(1);
//                }
//            }
//        }
//    }
//
//    private static boolean isKafkaAvailable(Properties props) {
//        try (AdminClient admin = AdminClient.create(props)) {
//            String clusterId = admin.describeCluster().clusterId().get();
//            System.out.println("✅ Kafka is available. Cluster ID: " + clusterId);
//            return true;
//        } catch (Exception e) {
//            System.err.println("❌ Kafka check failed: " + e.getMessage());
//            return false;
//        }
//    }
//}



import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerRetryExit {

    private static final String TOPIC_NAME = "my-topic"; // Replace with your topic name
    private static final int MAX_RETRIES = 5;
    private static final long BASE_RETRY_INTERVAL_MS = 2000;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Change this if needed
        props.put("group.id", "retry-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Step 1: Kafka health check
        if (!isKafkaAvailable(props)) {
            System.err.println("❌ Kafka is not available. Exiting...");
            System.exit(1);
        }

        // Step 2: Retry Kafka consumer connection
        int retryCount = 0;

        while (retryCount < MAX_RETRIES) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC_NAME));
                System.out.println("✅ Connected to Kafka and subscribed to topic: " + TOPIC_NAME);

                // You can choose to return or exit if no further processing is needed
                return;

            } catch (Exception e) {
                retryCount++;
                System.err.println("⚠️ Kafka connection failed (Attempt " + retryCount + "): " + e.getMessage());

                if (retryCount >= MAX_RETRIES) {
                    System.err.println("❌ Max retry attempts reached. Exiting application.");
                    System.exit(1);
                }

                try {
                    long backoff = BASE_RETRY_INTERVAL_MS * retryCount;
                    System.out.println("⏳ Waiting " + backoff + " ms before retrying...");
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    System.err.println("❌ Retry interrupted. Exiting.");
                    System.exit(1);
                }
            }
        }
    }

    // Check if Kafka broker is available
    private static boolean isKafkaAvailable(Properties props) {
        // These properties are not used by AdminClient and will be ignored
        // Remove consumer-specific configs if you want to avoid logs
//        Properties adminProps = new Properties();
//        adminProps.put("bootstrap.servers", props.getProperty("bootstrap.servers"));

        try (AdminClient admin = AdminClient.create(props)) {
            DescribeClusterResult result = admin.describeCluster();
            String clusterId = result.clusterId().get(5, TimeUnit.SECONDS);
            System.out.println("✅ Kafka is available. Cluster ID:and ip " + clusterId + props.getProperty("bootstrap.servers").toString());
            return true;
        } catch (Exception e) {
            System.err.println("❌ Kafka health check failed: " + e.getMessage());
            return false;
        }
    }
}

