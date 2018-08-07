package com.min.kafka.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @program: kafka
 * @description:
 * @author: mcy
 * @create: 2018-08-03 17:51
 **/
public class MyConsumer {

    private static KafkaConsumer<String ,String> consumer;
    private static Properties kafkaPropes;
    static {
        //基本配置
        kafkaPropes = new Properties();
        kafkaPropes.put("bootstrap.servers", "192.168.0.124:9092");
        kafkaPropes.put("group.id", "KafkaStudy");
        kafkaPropes.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPropes.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    /*
    * 自动提交位移
    * */
    public static void generalConsumeMessageAutoCommit(){
        kafkaPropes.put("enable.auto.commit",true);//设置自动提交
        consumer = new KafkaConsumer<>(kafkaPropes);
        consumer.subscribe(Collections.singletonList("min"));//订阅toptic
        try{
            while(true){
                boolean flag = true;
                ConsumerRecords<String, String> records = consumer.poll(100);//等待时间 在这里自动提交 默认5秒提交一次
                for(ConsumerRecord<String, String> record :records){
                    System.out.println(record.value());
                    if("done".equals(record.value())){
                        flag = false;
                    }
                }
                if(!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }


    /**
     * 手动提交位移( 同步 和 异步 )
     */
    private static void generalConsumerMessageSyncCommit(){
        kafkaPropes.put("auto.commit.offset",false);//设置自动提交
        consumer = new KafkaConsumer<>(kafkaPropes);
        consumer.subscribe(Collections.singletonList("min"));//订阅toptic
        try{
            while(true){
                boolean flag = true;
                ConsumerRecords<String, String> records = consumer.poll(100);

                for(ConsumerRecord<String, String> record :records){
                    System.out.println(record.value());
                    if("done".equals(record.value())){
                        flag = false;
                    }
                }

                //手动提交位移
                try{
                    consumer.commitSync();//手动同步提交
                    //consumer.commitAsync();//手动异步提交
                }catch (CommitFailedException e){
                    System.out.println("commit failed error"+e.getMessage());
                }

                if(!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }

    /**
     * 手动异步提交位移消息带回调
     */
    private static void generalConsumerMessageAsyncCommitWithCallback(){
        kafkaPropes.put("auto.commit.offset",false);//设置自动提交
        consumer = new KafkaConsumer<>(kafkaPropes);
        consumer.subscribe(Collections.singletonList("min"));//订阅toptic
        try{
            while(true){
                boolean flag = true;
                ConsumerRecords<String, String> records = consumer.poll(100);

                for(ConsumerRecord<String, String> record :records){
                    System.out.println(record.value());
                    if("done".equals(record.value())){
                        flag = false;
                    }
                }

                //手动提交位移
                try{
                    consumer.commitAsync((offset,e)->{
                        if(e!=null){
                            System.out.println("提交有异常发生");
                        }
                    });//手动异步提交
                }catch (CommitFailedException e){
                    System.out.println("commit failed error"+e.getMessage());
                }

                if(!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }

    }

}
