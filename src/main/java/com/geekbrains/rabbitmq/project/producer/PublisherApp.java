package com.geekbrains.rabbitmq.project.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PublisherApp {
    private static final String EXCHANGE_NAME = "programming_blog";
    private static final String TOPIC_ROOT_TITLE = "programming";
    private static final String TOPIC_DELIMETER = ".";

    public static void main(String[] argv) throws Exception {
        String[] routingKey = {"java", "c++", "python"};
        String messageHeader = "Message from";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            for(int j = 1; j < 11; j++) {
                for (int i = 0; i < routingKey.length; i++) {
                    String messageToPublish = String.format("%s %s number %d", messageHeader, routingKey[i], j);
                    channel.basicPublish(EXCHANGE_NAME, TOPIC_ROOT_TITLE + TOPIC_DELIMETER + routingKey[i],
                            null, messageToPublish.getBytes("UTF-8"));
                    System.out.println(" [x] Sent '" + routingKey[i] + "':'" + messageToPublish + "'");
                }
            }
        }
    }
}
