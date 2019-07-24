package com.sonu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import java.util.*;
public class SyncProducer {

    private static String TOPIC_NAME = "hello-topic1";
    public void produce() {

        Producer<String, String> producer = new KafkaProducer <>(createProperties());

        ProducerRecord<String, String> record = new
            ProducerRecord<>(TOPIC_NAME, "SyncKey", "SyncMessage");

        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println("Message is sent to Partition no " +
                recordMetadata.partition() + " and offset " + recordMetadata.offset());

        } catch (Exception e) {
            System.out.println("Exception--> "+ e);
        } finally{
            producer.close();
        }

    }
    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;

    }

}
