package com.ft.kafka01;

import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 创建人：taofut
 * 创建时间：2019-08-21 18:49
 * 描述：
 */
public class FutKafkaProducer extends Thread{

    KafkaProducer<Integer,String> producer;
    String topic;

    public FutKafkaProducer(String topic){
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.63.138:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"fut_producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 异步 批量发送 数据先保存内存，如果达到一定量则发送
//        properties.put(ProducerConfig.BATCH_SIZE_CONFIG);
        // 异步 间隔时间 数据没达到要求的数量，超过间隔时间也会发送
//        properties.put(ProducerConfig.LINGER_MS_CONFIG);
        producer=new KafkaProducer<Integer, String>(properties);
        this.topic=topic;
    }

    @Override
    public void run() {
        int num=0;
        while (num<20){
            try {
                String msg="fut_kafka"+num;
                // get得到发送的结果  同步
//                RecordMetadata metadata=producer.send(new ProducerRecord<Integer, String>(topic,msg)).get();
//                System.out.println(metadata.offset()+"->"+metadata.partition()+"->"+metadata.topic());

                // 如果要异步 则需要回调函数
                producer.send(new ProducerRecord<Integer, String>(topic, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        System.out.println(metadata.offset()+"->"+metadata.partition()+"->"+metadata.topic());
                    }
                });
                TimeUnit.SECONDS.sleep(2);
                ++num;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        new FutKafkaProducer("fut_test01").start();
    }
}
