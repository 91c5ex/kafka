package com.pratice;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerT {

    private static final Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    private static int count = 0;

    public static void main(String[] args) {





        Properties properties = new Properties();
        properties.put("bootstrap.servers","0:9092");
        properties.put("group.id","CountryCounter");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        Properties properties2 = new Properties();

        properties2.put("group.id","CountryCounter");

        properties2.put("bootstrap.servers", "0:9092");

        properties2.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties2.put("value.deserializer",
                "com.pratice.CustomerDeserializer");


//        Map<String,String> data = new HashMap<>();
        Map<String,Customer> data = new HashMap<>();

//        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        KafkaConsumer<String,Customer> consumer = new KafkaConsumer<String, Customer>(properties2);

        consumer.subscribe(Collections.singletonList("test"));

        final Thread mainThread = Thread.currentThread();


        Runtime.getRuntime().addShutdownHook(new Thread(){

            @Override
            public void run() {
                System.out.println("Starting Exit.....");

                consumer.wakeup();

                try{
                    mainThread.join();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });



        try{
            while(true){
//                ConsumerRecords<String,String> records = consumer.poll(100);
//                for(ConsumerRecord<String,String> record : records){
//                    System.out.printf("topic = %s, parition = %s, offset = %d, key = %s,value = %s \n",
//                            record.topic(),record.partition(),record.offset(),record.key(),record.value());
//
//                    int updateCount = 1;
//
//                    currentOffset.put(new TopicPartition(record.topic(),record.partition()),
//                            new OffsetAndMetadata(record.offset()+1,"no metadata"));
//
//                    if(count % 1000 ==1 ){
//                        consumer.commitAsync(currentOffset,null);
//                    }
//
//
//                    if(data.containsKey(record.key())){
//                        updateCount = updateCount + 1;
//
//                    }
//
//                    data.put(record.key(),record.value());
//                }

                ConsumerRecords<String,Customer> records = consumer.poll(100);
                for(ConsumerRecord<String,Customer> record : records){
                    System.out.printf("topic = %s, parition = %s, offset = %d, key = %s,value = %s \n",
                            record.topic(),record.partition(),record.offset(),record.key(),record.value());

                    int updateCount = 1;

                    currentOffset.put(new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()+1,"no metadata"));

                    if(count % 1000 ==1 ){
                        consumer.commitAsync(currentOffset,null);
                    }


                    if(data.containsKey(record.key())){
                        updateCount = updateCount + 1;

                    }

                    data.put(record.key(),record.value());
                }


//                try{
//                    consumer.commitSync();
//                }catch (CommitFailedException e){
//                    e.printStackTrace();
//                }

                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map,
                                           Exception e) {

                        if(e != null){
                            System.out.println("Commit Faild for offset {} " + map + "  " +e);
                        }

                    }
                });


            }
        }
        catch (WakeupException wk){
            System.out.println("ignoring Wakeup Exception  ");

        }
        finally {
            System.out.println("Closing Counsumer   ");
            consumer.close();
        }



    }

}
