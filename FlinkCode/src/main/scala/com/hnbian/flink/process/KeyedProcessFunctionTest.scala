package com.hnbian.flink.process

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description 测试 KeyedProcessFunction
  * 监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升， 则报警。
  * @Date 2020/8/31 17:38 
  **/

case class SensorReading(id:String,timestamp:Long,temperature:Double)

/**
1,1598971551,45
1,1598971552,43
1,1598971555,41
1,1598971580,40
1,1598971581,41
  */

object KeyedProcessFunctionTest {
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
    val processStream = stream2.keyBy(_.id).process(new TempIncreaseAlertFunction)
    stream1.print()
    processStream.print()
    env.execute()
  }
}



/**
  * 判断温度连续十秒一直上升则报警
  * KeyedProcessFunction[String, SensorReading, String]
  * String, 输入的 key 的数据类型
  * SensorReading, 输入的数据类型
  * String 输出的数据类型
  */
class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个值状态，保存上一个传感器温度值
  lazy val lastTemp: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))


  // 保存注册的定时器的时间戳
  lazy val currentTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  /**
    * 处理数据的主要逻辑
    * @param r
    * @param ctx
    * @param out 输出的数据
    */
  override def processElement(r: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    // 取出上一次的温度
    val prevTemp = lastTemp.value()
    println("prevTemp:"+prevTemp)
    // 将当前温度更新到上一次的温度这个变量中
    lastTemp.update(r.temperature)
    // 获取上一个定时器的时间戳
    val curTimerTimestamp = currentTimer.value()

    if (prevTemp == 0.0 || r.temperature < prevTemp) { // 温度下降或者是第一个温度值

      println("温度下降，删除定时器")
      // 删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)

      // 清空状态变量
      currentTimer.clear()
    } else if (r.temperature > prevTemp && curTimerTimestamp == 0) { // 温度上升且我们并没有设置定时器
      // 获取当前时间 + 10 秒
      val timerTs = ctx.timerService().currentProcessingTime() + 10000

      println("温度上升，更新定时器")
      // 注册定时器
      ctx.timerService().registerProcessingTimeTimer(timerTs)

      // 更新定时器
      currentTimer.update(timerTs)
    }

  }
   // # 投射类型
  override def onTimer(ts: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    ctx.timerService()
    out.collect("传感器 id 为: " + ctx.getCurrentKey + "的传感器温度值已经连续 1s 上升了。")
    currentTimer.clear()
  }
}
