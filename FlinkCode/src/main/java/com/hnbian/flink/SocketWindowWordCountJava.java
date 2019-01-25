package com.hnbian.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 功能说明: 消费socket数据计算5秒内WordCount
 * author: haonan.bian
 * date: 2018/10/23 16:40
 * yum install nc
 * nc -lp 8965
 */
public class SocketWindowWordCountJava {
    public static void main(String args[]) {
        try {

            //定义连接端口
            final int port = 8965;

            // 得到执行环境对象
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //连接套socket之后，读取输入data
            DataStream<String> text = env.socketTextStream("slave", port, "\n");

            // parse the data, group it, window it, and aggregate the countsDataStream
            DataStream<WordWithCount> windowCounts = text
                    .flatMap(new FlatMapFunction<String, WordWithCount>() {
                        public void flatMap(String value, Collector<WordWithCount> out) {
                            for (String word : value.split("\\s")) {
                                out.collect(new WordWithCount(word, 1L));
                            }
                        }
                    })
                    .keyBy("word")
                    .timeWindow(Time.seconds(5), Time.seconds(1))
                    .reduce(new ReduceFunction<WordWithCount>() {
                        public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                            return new WordWithCount(a.word, a.count + b.count);
                        }
                    });

            windowCounts.print().setParallelism(1);
            env.execute("Socket Window WordCount");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
		 }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String toString() {
            return word + " : " + count;
        }
    }
}
