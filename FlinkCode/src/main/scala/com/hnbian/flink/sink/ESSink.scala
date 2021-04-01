package com.hnbian.flink.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.mutable.ArrayBuffer

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020-07-27 22:47 
  **/
object ESSink {

  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("/opt/apache-maven-3.6.0/conf/settings.xml")

    val httpHosts = new util.ArrayList[HttpHost]()

    httpHosts.add(new HttpHost("node1",9200))

    // 创建 es sink 的 builder
    val esSinkBuilder  = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        /**
          * 发送数据的方法
          * @param t 要发送的数据
          * @param runtimeContext 上下文
          * @param requestIndexer 发送操作请求
          */
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          // 将数据包装成json或者Map
          val map = new java.util.HashMap[String,String]()
          map.put(t.length.toString,t.toString)

          // 创建 index request 准备发送数据

          val indesRequest = Requests.indexRequest()
            .index("data_length")
            .`type`("flinkData")
            .source(map)

          // 使用 requestIndexer 发送 HTTP 请求保存数据
          requestIndexer.add(indesRequest)
        }
      })


    stream.addSink(esSinkBuilder.build())

    env.execute("ESSink")
  }
}
