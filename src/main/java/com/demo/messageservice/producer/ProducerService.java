package com.demo.messageservice.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerService {

    private static final String TOPIC = "messageservice-topic";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = null;

        try {
            producer = new KafkaProducer<>(props);

            List<ProducerRecord<String, String>> records = new ArrayList<>();
            records.add(new ProducerRecord<>(TOPIC, "key1", "value1"));
            records.add(new ProducerRecord<>(TOPIC, "key2", "value2"));
            records.add(new ProducerRecord<>(TOPIC, "key3", "value3"));
            records.add(new ProducerRecord<>(TOPIC, "key4", "value4"));
            records.add(new ProducerRecord<>(TOPIC, "key5", "value5"));
            records.add(new ProducerRecord<>(TOPIC, "key6", "value6"));
            records.add(new ProducerRecord<>(TOPIC, "key7", "value7"));
            records.add(new ProducerRecord<>(TOPIC, "key8", "value8"));
            records.add(new ProducerRecord<>(TOPIC, "key9", "value9"));
            records.add(new ProducerRecord<>(TOPIC, "key10", "value10"));

            for (ProducerRecord record : records) {
                producer.send(record);
            }
        } catch (Exception e) {
            System.err.println("Error creating or sending producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the producer
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                    System.err.println("Error closing producer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}