package com.example.kafkaclient;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerService {

    private static final String TOPIC = "my-topic"; // Replace with your topic name
    private static final String BOOTSTRAP_SERVERS = "10.0.7.144:9092"; // Update if needed

    public void produceToSpecificPartition(int partition) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("💬 Enter messages to send to partition " + partition + " (type 'exit' to quit):");

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine();
                if ("exit".equalsIgnoreCase(input)) break;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, partition, null, input);
                producer.send(record);
                System.out.println("✅ Sent to partition " + partition + ": " + input);
            }

        } catch (Exception e) {
            System.err.println("❌ Error sending to partition: " + e.getMessage());
        }
    }
}

