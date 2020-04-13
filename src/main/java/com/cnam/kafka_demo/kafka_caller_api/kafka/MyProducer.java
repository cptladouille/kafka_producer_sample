package com.cnam.kafka_demo.kafka_caller_api.kafka;

import org.apache.kafka.clients.producer.Callback;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

import java.util.concurrent.ExecutionException;



public class MyProducer
{

    private final KafkaProducer producer;

    private int _messageNo;

    private final String _topicName;

    public static final String KAFKA_SERVER_URL = "localhost";

    public static final int KAFKA_SERVER_PORT = 9092;

    public static final String CLIENT_ID = "SampleProducer";

    public MyProducer(String topicName) {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);

        properties.put("client.id", CLIENT_ID);

        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(properties);

        this._topicName = topicName;

        this._messageNo = 0;
    }

    public void Send(String messageStr) {

        long startTime = System.currentTimeMillis();

            try {

                producer.send(new ProducerRecord(_topicName,

                        _messageNo,

                        messageStr)).get();

                System.out.println("Message envoyé: (" + _messageNo + ", " + messageStr + ")");

                _messageNo++;

            } catch (InterruptedException | ExecutionException e) {

                e.printStackTrace();

                // handle the exception

            }
    }
}
