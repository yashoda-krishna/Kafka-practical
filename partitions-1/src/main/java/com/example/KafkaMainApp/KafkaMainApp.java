package com.example.KafkaMainApp;

import com.example.kafkaclient.KafkaConsumerService;
import com.example.kafkaclient.KafkaProducerService;

import java.util.Scanner;

public class KafkaMainApp {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        KafkaProducerService producerService = new KafkaProducerService();
        KafkaConsumerService consumerService = new KafkaConsumerService();

        while (true) {
            System.out.println("""
                    🧠 Kafka Partition Console App
                    Commands:
                    → produce : send message to specific partition
                    → consume : read messages from specific partition
                    → exit    : quit
                    """);

            System.out.print("👉 Enter command: ");
            String command = scanner.nextLine().trim().toLowerCase();

            switch (command) {
                case "produce" -> {
                    System.out.print("🔢 Enter partition number to produce to: ");
                    int partition = Integer.parseInt(scanner.nextLine());
                    producerService.produceToSpecificPartition(partition);
                }
                case "consume" -> {
                    System.out.print("🔢 Enter partition number to consume from: ");
                    int partition = Integer.parseInt(scanner.nextLine());
                    consumerService.consumeFromPartition(partition);
                }
                case "exit" -> {
                    System.out.println("👋 Exiting...");
                    return;
                }
                default -> System.out.println("❓ Invalid command. Try again.");
            }
        }
    }
}
