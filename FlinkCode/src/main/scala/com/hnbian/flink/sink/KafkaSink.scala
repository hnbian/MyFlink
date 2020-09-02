package com.hnbian.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020-07-27 20:57 
  **/
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val brokers = "node1;992,node2:9092,node3:9093"
    val topic = "flink"

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("/opt/apache-maven-3.6.0/conf/settings.xml")

    stream.addSink(new FlinkKafkaProducer011[String](brokers,topic,new SimpleStringSchema()))

    env.execute("kafka sink ")

  }

}
