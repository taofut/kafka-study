package com.ft.kafka01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 创建人：taofut
 * 创建时间：2019-08-21 18:49
 * 描述：
 */
public class FutKafkaConsumer extends Thread{

    KafkaConsumer<Integer,String> consumer;
    String topic;

    public FutKafkaConsumer(String topic){
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.63.138:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"fut_consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"fut-gid");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000");
        // 消息消费完了告诉kafka，这一kafka就知道我这些被消费掉了，后期会作废
        // 这里是批量消费时的间隔时间 批量确认 自动提交
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消费昨天发布的数据
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer=new KafkaConsumer<Integer, String>(properties);
        this.topic=topic;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singleton(this.topic));
        while (true){
            ConsumerRecords<Integer,String> records=consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord record:records){
                System.out.println("key= "+record.key()+" value ="+record.value()+" offset= "+record.offset());
                System.out.println("--------------------");
            }
        }

    }

    public static void main(String[] args) {
        new FutKafkaConsumer("fut_test01").start();
    }
}
