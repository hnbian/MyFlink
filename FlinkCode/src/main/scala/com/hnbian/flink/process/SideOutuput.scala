package com.hnbian.flink.process

import org.apache.flink.streaming.api.functions.{ProcessFunction}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @Author haonan.bian
  * @Description 侧输出流
  * @Date 2020/8/31 17:51 
  **/
object SideOutuput {

    def main(args: Array[String]): Unit = {
      // 创建执行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val stream1: DataStream[String] = env.socketTextStream("localhost", 9999)

      val stream2: DataStream[SensorReading] = stream1.map(data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading) = element.timestamp * 1000
      })

      val monitoredReadings: DataStream[SensorReading] = stream2.process(new FreezingMonitor())

     monitoredReadings
       .getSideOutput(new OutputTag[String]("freezing-alarms"))
       .print()

      env.execute()
    }


  //接下来我们实现 FreezingMonitor 函数，用来监控传感器温度值，将温度值低于32F 的温度输出到 side output。

  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {

    // 定义一个侧输出标签
    lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")
    override def processElement(r: SensorReading,
                                ctx: ProcessFunction[SensorReading,
                                  SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 温度在 32F 以下时，输出警告信息

      if (r.temperature < 32.0) {
        ctx.output(freezingAlarmOutput, s"告警：温度低于 0 度， ${r.id}")
      }

      // 所有数据直接常规输出到主流
      out.collect(r)
    }
  }
}
