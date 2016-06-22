package com.wzq.rabbitmq.helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Author: zhenqing.wang <wangzhenqing1008@163.com>
 * Date: 2016-06-21 18:56:24
 * Description: 生产者
 */
public class HProducer {

    private static final String QUEUE_NAME = "wangzq";

    public void send() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        String message = "Hello World";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));

        System.out.println("[Producer] send Message " + message);

        channel.close();
        connection.close();
    }

    public static void main(String[] args) {
        HProducer producer = new HProducer();
        try {
            producer.send();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
