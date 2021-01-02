package com.hnbian.flink.window

import com.hnbian.flink.watermark.TimeStamp
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/1 16:14 
  **/
object TumblingEventTimeWindow extends App{

  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignAscendingTimestamps(_.time)

  stream2.keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(new minAgeFunction2)
    .print("TumblingEventTimeWindow")

  environment.execute()
}
case class Obj1(id:String,name:String,time:Long)

// 计算窗口内年纪最小的记录
class minAgeFunction2 extends ReduceFunction[Record]{
  override def reduce(r1: Record, r2: Record):Record = {
    if(r1.age < r2.age){
      r1
    }else{
      r2
    }
  }
}