package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionProducer {

    public static void main(String[] args) {
        //1.创建kafka生产者的配置信息
        Properties properties = new Properties();//ctrl+alt+v
        properties.put("bootstrap.servers","hadoop101:9092");//kafka集群，broker-list
        properties.put(ProducerConfig.ACKS_CONFIG,"all");//应答级别
        properties.put("retries",3);//重试次数
        properties.put("batch.size",16384);//批次大小
        properties.put("linger.ms",1);//等待时间
        properties.put("buffer.memory",33554432);//缓冲区大小
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.partitioner.MyPartitioner");//添加分区器
        //key,value的序列化流
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //发送数据
        for(int i=0;i<10;i++){

            producer.send(new ProducerRecord<>("first", "atguigu---" + i), (metadata, e) ->
                    System.out.println(metadata.partition()+"----"+metadata.offset()));
        }
        //关闭资源
        producer.close();
    }

}
