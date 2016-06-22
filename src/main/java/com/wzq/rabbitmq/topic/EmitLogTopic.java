package com.wzq.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Author: zhenqing.wang <wangzhenqing1008@163.com>
 * Date: 2016-06-22 16:16:04
 * Description: topic
 */
public class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public void send() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        String[] routingKeys = new String[]{"quick.orange.rabbit",
                "lazy.orange.elephant",
                "quick.orange.fox",
                "lazy.brown.fox",
                "quick.brown.fox",
                "quick.orange.male.rabbit",
                "lazy.orange.male.rabbit"};

        for (String topic : routingKeys) {
            String message = "From " + topic + " routingKey' s message!";
            channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes());
            System.out.println(" [x] Sent '" + topic + "':'" + message + "'");
        }
        channel.close();
        connection.close();
    }

    public static void main(String[] args) {
        EmitLogTopic topic = new EmitLogTopic();
        try {
            topic.send();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
