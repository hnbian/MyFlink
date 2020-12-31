//package com.hnbian.flink.sink
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//import org.apache.flink.streaming.connectors.redis.RedisSink
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
//import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
//
///**
//  * @Author haonan.bian
//  * @Description //TODO
//  * @Date 2020-07-27 22:47
//  **/
//object RedisSink {
//  def main(args: Array[String]): Unit = {
//
//    // 创建执行环境
////    val env = StreamExecutionEnvironment.getExecutionEnvironment
////    env.setParallelism(1)
////
////    val stream = env.readTextFile("/opt/apache-maven-3.6.0/conf/settings.xml")
////
////    val config = new FlinkJedisPoolConfig.Builder()
////      .setHost("localhost")
////      .setPort(6379)
////      .build()
////
////    stream.addSink(new RedisSink[String](config,new Mapper))
////
////    env.execute("Redis sink ")
//  }
//
//}
//
///**
//  * 定义发送数据的 Mapper
//  */
//class Mapper() extends RedisMapper[String]{
//  /**
//    * 定义保存数据到 Redis 的命令
//    * @return
//    */
//  override def getCommandDescription: RedisCommandDescription = {
//    // 将数据保存成哈希表 k->v
//
//   // new RedisCommandDescription(RedisCommand.HSET,"data_length")
//
//
//  }
//
//  /**
//    * 定义保存到 Redis 的key
//    * @param t
//    * @return
//    */
//  override def getKeyFromData(t: String): String =t.length.toString
//
//  /**
//    * 定义保存到 Redis 的value
//    * @param t
//    * @return
//    */
//  //override def getValueFromData(t: String): String = t
//}
