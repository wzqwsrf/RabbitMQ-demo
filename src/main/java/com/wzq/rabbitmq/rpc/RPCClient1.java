package com.wzq.rabbitmq.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Author: zhenqing.wang <wangzhenqing1008@163.com>
 * Date: 2016-06-22 16:43:57
 * Description: RPC
 */
public class RPCClient1 {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    private QueueingConsumer consumer;

    public RPCClient1() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");

        connection = factory.newConnection();
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
    }

    public String call(String message) throws InterruptedException, IOException {
        String response = null;
        String corrID = java.util.UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties()
                .builder()
                .correlationId(corrID)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes());

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            if (delivery.getProperties().getCorrelationId().equals(corrID)) {
                response = new String(delivery.getBody());
                break;
            }
        }

        return response;
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RPCClient1 client = null;
        try {
            client = new RPCClient1();
            String ret = client.call("9");
            System.out.println(ret);
            client.close();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }
}
