package com.ft.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 创建人：taofut
 * 创建时间：2019-08-18 21:30
 * 描述：
 */
public class MyProducer {

    private static KafkaProducer<String,String> producer;

    static {
        Properties properties=new Properties();
        properties.put("bootstrap.servers","192.168.63.138:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer=new KafkaProducer<String, String>(properties);
    }

    /**
     * 生产者发送消息，只管发送，不管结果
     */
    private static void sendMessage(){
        ProducerRecord<String,String> record=new ProducerRecord<String, String>(
                "fut_test_api001","name","taofut000001"
        );
        producer.send(record);
        producer.close();

    }

    public static void main(String[] args) {
        sendMessage();
    }
}
