package com.sonu.kafka;

import com.sonu.kafka.consumer.ConsumerGroup;
import com.sonu.kafka.consumer.SimpleConsumer;
import com.sonu.kafka.producer.advanced.OrderedProducer;

public class App{

    public static void simpleProdConsumer() {

        //SyncProducer syncProducer = new SyncProducer();
        //syncProducer.produce();

        //AsyncProducer asyncProducer = new AsyncProducer();
        //asyncProducer.produce();

        Thread thread = new ConsumerGroup();
        thread.start();
        System.out.println("Creating another consumer");

        SimpleConsumer simpleConsumer2 = new SimpleConsumer();
        simpleConsumer2.consume();
    }

    public static void main(String...l) {

        OrderedProducer orderedProducer = new OrderedProducer();
        //Round robin
       // orderedProducer.produce(1);

        // Hash partitioner
        //orderedProducer.produce(2);

        // Custom paritioner
        orderedProducer.produce(3);

    }
}
