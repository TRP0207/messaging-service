package com.demo.messageservice.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;


public class ConsumerService {

    private static final String TOPIC = "messageservice-topic";
    private static final AtomicLong successCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC));
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

                    for (ConsumerRecord<String, String> record : records) {
                        processMessage(record);
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                        successCount.incrementAndGet();
                    }
                } finally {
                }
                logCounters();
            }
        } catch (Exception e) {
            System.err.println("Error creating or using consumer: " + e.getMessage());
            e.printStackTrace();
            errorCount.incrementAndGet();
        } finally {
            if (consumer != null) {
                try {
                    logCounters();
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("Error closing consumer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        System.out.println("Processing message: " + record.value());
        if (record.value().contains("error")) {
            throw new RuntimeException("Simulated processing error");
        }
    }

    private static void logCounters() {
        System.out.println("Total messages processed successfully: " + successCount.get());
        System.out.println("Total errors encountered: " + errorCount.get());
    }
}