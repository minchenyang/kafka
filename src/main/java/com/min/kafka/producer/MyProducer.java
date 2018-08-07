package com.min.kafka.producer;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;


/**
 * @program: kafka
 * @description:
 * @author: mcy
 * @create: 2018-08-03 09:50
 **/
public class MyProducer {

    private static KafkaProducer<String,String> producer;

    static {

        //基本配置
        Properties kafkaPropes = new Properties();
        kafkaPropes.put("bootstrap.servers", "192.168.0.125:9092");
        kafkaPropes.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPropes.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //自定义分区器
        kafkaPropes.put("partitioner.class", "com.min.kafka.partitioner.CustomPartition");

        producer = new KafkaProducer(kafkaPropes);
    }

    /** 三种发送的方式 */

    /*
    * 只发不管结果
    * */
    public static void sendMessageForgetResult(){
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "min","keyForgetResult","valueForgetResult"
        );
        producer.send(record);
        producer.close();
    }


    /*
    * 同步发送
    * 调用send方法  返回一个Future对象， 可以通过get方法判断发送是否成功
    * */
    public static void sendMessageSysnc() throws Exception{
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "min","keyForgetResult","sync"
        );
        RecordMetadata recordMetadata = producer.send(record).get();
        System.out.println(recordMetadata.topic());
        System.out.println(recordMetadata.partition());
        System.out.println(recordMetadata.offset());
        producer.close();
    }


    /*
    * 异步发送  调用send方法时提供一个回调方法，当接收到broker结果后回调此方法
    * */

    public static void sendMessageCallbock(){
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "min","keyForgetResult","callback"
        );
        producer.send(record,new MyProducerCallback());
        producer.close();
    }


    private static class MyProducerCallback implements Callback{

        @Override
        public void onCompletion(final RecordMetadata recordMetadata, final Exception e) {
            if(e!=null){
                e.printStackTrace();
                return ;
            }
            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.hasOffset());
        }
    }

}
