package com.pratice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaSpark {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Test-spark");

//        conf.setMaster("spark://localhost:8080");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaStreamingContext context = new JavaStreamingContext(sc, new Duration(1000));


//
//        JavaRDD<String> textFile = sc.textFile("/home/amit/Documents/spark.txt");
//
//        JavaPairRDD<String,Integer> element = textFile.flatMap(s->{
//            return Arrays.asList(s.split("|")).iterator();
//        }).mapToPair(word->{
//            return new Tuple2<>(word,1);
//        }).reduceByKey((a, b) -> a + b);


//        System.out.println(element.collectAsMap());


        /*Example Using kafka*/


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");


        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent()
                        , ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


        JavaPairDStream<String, String> pair = stream.mapToPair(record -> {
            System.out.println(record.key() + record.value());
            return new Tuple2<>(record.key(), record.value());
        });

//        JavaDStream<String> stream1 = stream.map(s->s.value());
//
//        stream1.print();


       // System.out.println(pair.toString());
        stream.foreachRDD(rdd->{
            OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();

            rdd.foreachPartition(conumerRecords ->{
                 OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                System.out.println(o.topic() + "   " + o.partition() + "  " + o.fromOffset() + "   " + o.untilOffset());
                conumerRecords.forEachRemaining(c-> System.out.println(c.value()));
            });


            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);

        });







//        OffsetRange[] offsetRanges = {
//                OffsetRange.create("test",0,0,100),
//                OffsetRange.create("test",1,0,100),
//                OffsetRange.create("test",2,0,100),
//                OffsetRange.create("test",3,0,100),
//                OffsetRange.create("test",4,0,100),
//                OffsetRange.create("test",5,0,100),
//                OffsetRange.create("test",6,0,100),
//                OffsetRange.create("test",7,0,100),
//                OffsetRange.create("test",8,0,100),
//                OffsetRange.create("test",9,0,100),
//                OffsetRange.create("test",10,0,100),
//        };

//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//        JavaRDD<ConsumerRecord<String,String>> rdd = KafkaUtils.createRDD(sc,kafkaParams,
//              offsetRanges,LocationStrategies.PreferConsistent());
//
//        rdd.collect().forEach(s-> System.out.println(s.value()));



        context.start();
        context.awaitTermination();

    }

}
