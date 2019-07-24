package com.sonu.kafka.producer.advanced;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderedProducer implements Callback {

    private static String TOPIC_NAME = "hello-topic1";
    private static Scanner in;
    public void produce(int partitioning) {

        Producer<String, String> producer = new KafkaProducer<>(createProperties(partitioning));
        in = new Scanner(System.in);
        String line = in.nextLine();
        while (!line.equals("exit")) {
            String[] keyValue = line.split("-");
            String countryId = keyValue[0];
            String orderData = keyValue[1];
            ProducerRecord<String, String> rec;
            if (partitioning==2) {
                rec = new ProducerRecord<String, String>(TOPIC_NAME, countryId, orderData);
            } else {
                rec = new ProducerRecord<String, String>(TOPIC_NAME, orderData);
            }
            producer.send(rec);
            line = in.nextLine();

        }
        in.close();
        producer.close();

    }
    private static Properties createProperties(int partitioning) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091,localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if (partitioning == 3) {
            properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getCanonicalName());
        }
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
