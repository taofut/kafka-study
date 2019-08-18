package com.ft.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 创建人：taofut
 * 创建时间：2019-08-18 21:30
 * 描述：
 */
public class MyProducer02 {

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

    /**
     * 同步发送，并获取发送结果
     * @throws Exception
     */
    private static void sendMessageSync() throws Exception{
        ProducerRecord<String,String> record=new ProducerRecord<String, String>(
                "fut_test_api001","name","sync"
        );
        RecordMetadata result=producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());

        producer.close();

    }

    /**
     * 异步发送
     */
    private static void sendMessageCallback(){
        ProducerRecord<String,String> record=new ProducerRecord<String, String>(
                "fut_test_api001","name","callback"
        );
        producer.send(record,new MyProducerCallback());
        producer.close();
    }

    /**
     * 回调类
     */
    private static class MyProducerCallback implements Callback{
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e!=null){
                e.printStackTrace();
                return;
            }
            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }

    public static void main(String[] args) throws Exception{
//        sendMessage();
//        sendMessageSync();
        sendMessageCallback();
    }
}
