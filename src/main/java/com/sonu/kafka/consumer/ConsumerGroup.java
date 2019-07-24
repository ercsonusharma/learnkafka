package com.sonu.kafka.consumer;

public class ConsumerGroup extends Thread {

    public void run(){

        System.out.println("Creating a consumer");
        SimpleConsumer simpleConsumer1= new SimpleConsumer();
        simpleConsumer1.consume();
        System.out.println("completed creating  consumer");
    }

}
