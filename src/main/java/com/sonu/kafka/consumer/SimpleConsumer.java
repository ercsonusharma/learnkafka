package com.sonu.kafka.consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class SimpleConsumer {

    private static String TOPIC_NAME = "Tweets";
    private static String GROUP_NAME = "TrendingTweets";

    public void consume() {

        KafkaConsumer<String, String> consumer = null;

        consumer = new KafkaConsumer<>(createProperties());
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord record : records) {
                System.out.printf(
                    "Message received -> partition = %d, offset = %d," + " key = %s, value = %s\n",
                    record.partition(), record.offset(), record.key(), record.value());
                System.out.println("Response from Consumer connected to Partition:");
                for (TopicPartition tp : consumer.assignment()) {
                    System.out.println(tp.partition() + "\n");

                }
            }
        }


    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("group.id", GROUP_NAME);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;

    }
}
