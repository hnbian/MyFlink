package com.hnbian.flink.watermark

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/1 21:20 
  **/
object EventTimeAssignAscendingTimestamps extends App{

  // 创建执行环境
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  private val stream: DataStream[String] = env.socketTextStream("localhost", 9999)
  stream.map(data=>{
    val dataArray: Array[String] = data.split(",")
    Obj1(dataArray(0),dataArray(1),dataArray(2).toLong)
  }).assignAscendingTimestamps(data=>{data.time}) // 设置时间语义中的时间戳字段（毫秒值）

  //stream.assignTimestamps()
  env.execute()
}