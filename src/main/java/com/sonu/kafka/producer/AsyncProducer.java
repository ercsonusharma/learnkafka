package com.sonu.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class AsyncProducer implements Callback {

    private static String TOPIC_NAME = "hello-topic1";

    public void produce() {

        Producer<String, String> producer = new KafkaProducer<>(createProperties());

        ProducerRecord<String, String> record =
            new ProducerRecord<>(TOPIC_NAME, "AsyncKey", "ASyncMessage");

            producer.send(record);
            System.out.println("Message Sent");
            producer.close();

    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;

    }
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            System.out.println("AsyncProducer failed with an exception");
        else
            System.out.println("Async Message is sent to Partition no "
                + recordMetadata.partition() + " and offset " + recordMetadata.offset());
    }
}
