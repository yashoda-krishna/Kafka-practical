package com.example.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import java.util.Properties;
import java.util.Scanner;

@Service
public class KafkaProducerService {

    private static final String TOPIC = "my-topic"; // Replace with your topic
    private static final String BOOTSTRAP_SERVERS = "10.0.7.144:9092";

    @PostConstruct
    public void startProducing() {
        new Thread(this::produceFromConsole).start();
    }

    public void produceFromConsole() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("💬 Enter messages to send to Kafka (type 'exit' to quit):");

            int keyCounter = 0;

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine();

                if ("exit".equalsIgnoreCase(input)) {
                    System.out.println("👋 Exiting producer.");
                    break;
                }

                String key = String.valueOf(keyCounter++);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, input);
                producer.send(record);
                System.out.println("✅ Sent: " + input);
            }

        } catch (Exception e) {
            System.err.println("❌ Producer failed: " + e.getMessage());
        }
    }
}

