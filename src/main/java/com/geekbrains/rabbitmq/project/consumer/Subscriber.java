package com.geekbrains.rabbitmq.project.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Subscriber {
    private static final String EXCHANGE_NAME = "programming_blog";
    private static final String TOPIC_ROOT_TITLE = "programming";
    private static final String TOPIC_DELIMETER = ".";
    private static final int UNSUBSCRIBING_TOPIC_INDEX = 0;
    private static final int UNSUBSCRIBING_TOPIC_COUNT = 7;
    private int topicMessageCount = 0;

    private final String[] topics = {"java", "python"};

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String queueName;

    public void doReceive() throws IOException, TimeoutException {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        queueName = channel.queueDeclare().getQueue();
        System.out.println("My queue name: " + queueName);
        for(int i = 0; i < topics.length; i++) {
            String routingKey = TOPIC_ROOT_TITLE + TOPIC_DELIMETER + topics[i];
            System.out.println("Subscribing to channel: " + routingKey);
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }

        System.out.println(" [*] Waiting for messages");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            String fullTopicTitle = TOPIC_ROOT_TITLE + TOPIC_DELIMETER + topics[UNSUBSCRIBING_TOPIC_INDEX];
            if(fullTopicTitle.equals(delivery.getEnvelope().getRoutingKey())) checkSubscription();
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

    private void checkSubscription() throws IOException {
        if(topicMessageCount >= UNSUBSCRIBING_TOPIC_COUNT)
            channel.queueUnbind(queueName, EXCHANGE_NAME, TOPIC_ROOT_TITLE + TOPIC_DELIMETER + topics[UNSUBSCRIBING_TOPIC_INDEX]);
        else
            topicMessageCount++;
    }
}
