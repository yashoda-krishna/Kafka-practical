package com.example.kafkaclient;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerService {

    private static final String TOPIC = "my-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public void produceFromConsole() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("\n💬 Type messages to send. Type 'exit' to return to menu.");
            int keyCounter = 0;

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine();

                if ("exit".equalsIgnoreCase(input)) {
                    System.out.println("↩️ Returning to main menu...");
                    break;
                }

                String key = String.valueOf(keyCounter++);
                producer.send(new ProducerRecord<>(TOPIC, key, input));
                System.out.println("✅ Sent: " + input);
            }

        } catch (Exception e) {
            System.err.println("❌ Producer error: " + e.getMessage());
        }
    }
}
