package com.geekbrains.rabbitmq.project.consumer;

public class SubscriberApp {

    public static void main(String[] argv) throws Exception {
        Subscriber subscriber = new Subscriber();
        subscriber.doReceive();
    }
}
