//package com.example.demo;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//
//import java.util.Properties;
//
//public class KafkaProducerRetry {
//	private static final String TOPIC_NAME = "my-topic";
//    private static final int MAX_RETRIES = 5;
//    private static final long RETRY_INTERVAL_MS = 2000;
//
//    public static void main(String[] args) {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "local-host:9092");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("acks", "all");
//        props.put("retries", 0); // Let your code handle retries
//
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//            String topic = "my-topic";
//            String key = "key1";
//            String value = "Hello Kafka";
//
//            for (int i = 1; i <= MAX_RETRIES; i++) {
//                try {
//                    RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
//                    System.out.println("✅ Message sent to topic " + metadata.topic() + " at offset " + metadata.offset());
//                    break;
//                } catch (Exception e) {
//                    System.err.printf("⚠️ Send attempt %d failed: %s%n", i, e.getMessage());
//                    if (i == MAX_RETRIES) {
//                        System.err.println("❌ Max retries reached. Giving up.");
//                        System.exit(1);
//                    }
//                    Thread.sleep(RETRY_INTERVAL_MS * i);
//                }
//            }
//        } catch (Exception e) {
//            System.err.println("❌ Producer setup failed: " + e.getMessage());
//        }
//    }
//}
