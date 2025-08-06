package com.example.KafkaMainApp;


import com.example.kafkaclient.KafkaConsumerService;
import com.example.kafkaclient.KafkaProducerService;

import java.util.Scanner;

public class KafkaMainApp {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        KafkaProducerService producerService = new KafkaProducerService();
        KafkaConsumerService consumerService = new KafkaConsumerService();

        System.out.println("""
                🧠 Kafka Console App:
                → produce  : start producing messages
                → consume  : start consuming messages
                → exit     : stop everything & exit
                """);

        while (true) {
            System.out.print("👉 Enter command: ");
            String command = scanner.nextLine().trim().toLowerCase();

            switch (command) {
                case "produce" -> producerService.produceFromConsole();

                case "consume" -> {
                    Thread consumerThread = new Thread(consumerService::consumeFromKafka);
                    consumerThread.start();

                    System.out.println("🔁 Consumer running in background. Type 'stop' to halt.");
                    while (true) {
                        String input = scanner.nextLine().trim().toLowerCase();
                        if (input.equals("stop")) {
                            consumerService.stop();
                            consumerThread.interrupt();
                            System.out.println("⛔ Consumer stopped.");
                            break;
                        }
                    }
                }

                case "exit" -> {
                    System.out.println("👋 Exiting the application.");
                    System.exit(0);
                }

                default -> System.out.println("❓ Unknown command. Try 'produce', 'consume', or 'exit'.");
            }
        }
    }
}
