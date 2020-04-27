package com.hnbian.flink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.util.Properties;

/**
 * 获取wiki 数据 计算用户流量
 * 部署到服务器需要添加依赖jar
 * flink-connector-wikiedits_2.11-1.6.0.jar
 * flink-connector-kafka-base_2.11-1.6.0.jar
 * kafka-clients-0.9.0.1.jar
 * flink-connector-kafka-0.9_2.11-1.6.2.jar
 */
public class WikipediaAnalysis {

    private static final String brokers = "node2:6667,node3:6667,node4:6667";
    private static final String topic = "xy_bms_vehicle_pass_topic";

    public static void main(String[] args) {

        try {

            //创建StreamExecutionEnvironment ,可用于设置执行参数并创建从外部读取数据的Source
            StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
            //创建一个从Wikipedia IRC 读取数据的Source
            DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
            KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(
                    new KeySelector<WikipediaEditEvent, String>() {
                        public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                            return wikipediaEditEvent.getUser();
                        }
                    }
            );
            DataStream<Tuple2<String, Long>> result = keyedEdits
                    .timeWindow(Time.seconds(5))
                    .fold(new Tuple2<String, Long>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                        public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) throws Exception {
                            acc.f0 = event.getUser();
                            acc.f1 = Long.valueOf(event.getByteDiff());
                            return acc;
                        }
                    });
            //打印数据到控制台
            result.print();


            //发送数据到kafka
            Properties produceConfig = new Properties();
            produceConfig.put("bootstrap.servers", brokers);
            //这里要设置使用的默认分区类，否则会报使用分区类不一致的问题
            produceConfig.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
            result.map(new MapFunction<Tuple2<String, Long>, String>() {
                           public String map(Tuple2<String, Long> tuple) throws Exception {
                               System.out.println("println=>" + tuple.toString());
                               return tuple.toString();
                           }
                       }
            ).addSink(new FlinkKafkaProducer09<String>(topic, new SimpleStringSchema(), produceConfig));
            see.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * 1.控制台打印的数据
 * 4> (Anthere,4)
 * 4> (Spintendo,413)
 * 4> (Unoquha,233)
 * 2> (05:205:8A:DE41:389A:548E:2C97:AC69,41)
 * 3> (Zanhe,71)
 * 1> (Casliber,322)
 * 3> (9.94.72.112,0)
 * 3> (Vatsmaxed,-17)
 * 2> (Kyabe,-111)
 * 3> (InternetArchiveBot,179)
 * 3> (AnomieBOT,-372)
 * 4> (TardisTybort,992)
 * 4> (Mccapra,-29)
 * 4> (.146.39.109,21)
 * 4> (AllSaintsfan1,-17)
 * 1> (Kevinthk,-23)
 * 4> (4.149.125.191,8)
 * 3> (Lugnuts,11)
 * 1> (ParadiseDesertOasis8888,231)
 * 2.kafka 收到数据结果
 * (9.224.3.221,255)
 * (9.192.98.165,-1)
 * (Meno25,14)
 * (JJMC89,-3410)
 * (HBC AIV helperbot5,-365)
 * (Joshua Jonathan,3)
 * (Boothsift,-161)
 * (QuackGuru,-939)
 * (Meno25,14)
 * (Alphabetstreet,20)
 * (Chanheigeorge,208)
 * (02:3A80:E34:FBA3:79EB:55B5:908E:3F,-8)
 * (Wollers14,-1)
 * (David829,-2)
 * 2.kafka 收到数据结果
 * (9.224.3.221,255)
 * (9.192.98.165,-1)
 * (Meno25,14)
 * (JJMC89,-3410)
 * (HBC AIV helperbot5,-365)
 * (Joshua Jonathan,3)
 * (Boothsift,-161)
 * (QuackGuru,-939)
 * (Meno25,14)
 * (Alphabetstreet,20)
 * (Chanheigeorge,208)
 * (02:3A80:E34:FBA3:79EB:55B5:908E:3F,-8)
 * (Wollers14,-1)
 * (David829,-2)
 */
/** 2.kafka 收到数据结果
 (9.224.3.221,255)
 (9.192.98.165,-1)
 (Meno25,14)
 (JJMC89,-3410)
 (HBC AIV helperbot5,-365)
 (Joshua Jonathan,3)
 (Boothsift,-161)
 (QuackGuru,-939)
 (Meno25,14)
 (Alphabetstreet,20)
 (Chanheigeorge,208)
 (02:3A80:E34:FBA3:79EB:55B5:908E:3F,-8)
 (Wollers14,-1)
 (David829,-2)
 */
