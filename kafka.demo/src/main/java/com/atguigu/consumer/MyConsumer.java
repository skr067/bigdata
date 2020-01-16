package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        //1.创造消费者配置信息
        Properties properties = new Properties();
            //连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");
            //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
            //自动提交的延时
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
            //key,value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
            //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"data");
            //重置消费者offset
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//换组
        //2.创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //3.订阅主题
        consumer.subscribe(Collections.singletonList("first"));
        //consumer.subscribe(Arrays.asList("first","second"));
        //4.获取数据
        //while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            //5.解析并打印
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "---------" + consumerRecord.value());
            }
        //}
        //6.关闭连接
        consumer.close();
    }
}
