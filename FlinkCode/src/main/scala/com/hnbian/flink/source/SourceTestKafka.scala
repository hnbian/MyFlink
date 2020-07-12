package com.hnbian.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020-07-11 10:44 
  **/
object SourceTestKafka {
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()

    val topic = "text"

    properties.setProperty("bootstrap.servers","node1:9092")

    properties.setProperty("group.id","consumer-group1")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")


    val stream = env.addSource(new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),properties))

    // flink 消费数据可以把消费数据的 offset 作为状态保存起来
    // 恢复数据时可以将 offset 设置为恢复数据的位置。

    stream.print()

    env.execute()


  }

}
