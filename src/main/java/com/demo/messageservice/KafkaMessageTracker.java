package com.demo.messageservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaMessageTracker {
    private static final String TOPIC_NAME = "messageservice-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "my-group-id";

    // Atomic counters for thread-safe updates
    private static final AtomicLong successCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);

    public static void main(String[] args) {
        // Kafka consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create Kafka consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while (true) {
                // Poll for records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Process the message
                        processMessage(record);

                        // Increment success counter
                        successCount.incrementAndGet();

                    } catch (Exception e) {
                        // Log the error and increment the error counter
                        System.err.println("Error processing message: " + record.value());
                        e.printStackTrace();

                        errorCount.incrementAndGet();
                    }
                }

                // Log the counters periodically
                logCounters();
            }
        } catch (Exception e) {
            System.err.println("Error in Kafka consumer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        // Your message processing logic here
        System.out.println("Processing message: " + record.value());

        // Simulate an error for demonstration purposes
        if (record.value().contains("error")) {
            throw new RuntimeException("Simulated processing error");
        }
    }

    private static void logCounters() {
        System.out.println("Total messages processed successfully: " + successCount.get());
        System.out.println("Total errors encountered: " + errorCount.get());
    }
}
