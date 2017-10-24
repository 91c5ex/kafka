package com.pratice;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerT {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "0:9092");

        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        Properties properties2 = new Properties();

        properties2.put("bootstrap.servers", "0:9092");

        properties2.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties2.put("value.serializer",
                "com.pratice.CustomerSerializer");


        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        Producer<String, Customer> secondProducer = new KafkaProducer<String, Customer>(properties2);

        int i = 0;


        while (true) {
            System.out.println("value of i = " + i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("test", "k", "This is a value " + i);

            Customer c = new Customer(i, "amit");

            ProducerRecord<String, Customer> customerProducerRecord =
                    new ProducerRecord<String, Customer>("test", "k2", c);

            class DemoProducerCallback implements Callback {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println(recordMetadata.toString());
                }

            }


            try {

                RecordMetadata metadata = producer.send(record).get();
                System.out.println(metadata.toString());
//                producer.send(record, new DemoProducerCallback());
//                RecordMetadata metadata = secondProducer.send(customerProducerRecord).get();
//                System.out.println(metadata.toString());



            } catch (Exception e) {
                e.printStackTrace();
            }


            i++;
          //  Thread.sleep(1000);

        }


    }


}
