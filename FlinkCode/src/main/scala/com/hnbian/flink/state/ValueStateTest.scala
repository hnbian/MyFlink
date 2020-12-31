package com.hnbian.flink.state

//import com.hnbian.flink.process.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/9/1 17:52 
  **/
case class SensorReading(id:String,timestamp:Long,temperature:Double)
object ValueStateTest {
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
    stream1.print()


    // 使用 processFunction 方式
    //val stream3 = stream2.keyBy(_.id).process(new TempChangeAlert(2.0))
    //stream3.print("processFunction")

    // 使用 flatMap 方式操作
//    val stream4 = stream2.keyBy(_.id).flatMap(new TempChangeAlert(2.0))
//    stream4.print("flatMap RichFlatMapFunction")

    val threshold: Double = 2.0

    // 使用 flatMapWithState 实现检测两次的温度大于阈值则告警
     val stream5: DataStream[(String, Double, Double)]
     = stream2.keyBy(_.id)
       // 定义输出类型(String, Double, Double) 与状态类型
       .flatMapWithState[(String, Double, Double), Double] {
         // 如果没有状态的话，也就是首次进入，将当前温度值存到状态
         case (in: SensorReading, None) =>{
           (List.empty, Some(in.temperature))
         }

         // 如果有状态，则与当前温度与状态中的温度做比较
         case (r: SensorReading, lastTemp: Some[Double]) =>{
           val tempDiff = (r.temperature - lastTemp.get).abs
           if (tempDiff > threshold) {
             (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
           } else {
             (List.empty, Some(r.temperature))
           }
         }

       }

    stream5.print()

    env.execute()

  }


  /**
    * 使用 flat map 实现检测两次的温度大于阈值则告警
    * @param threshold
    */
  class TempChangeAlert(val threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    // 定义状态
    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      // 定义lastTempDescriptor
      val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
      // 初始化的时候声明 状态
      lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
    }

    override def flatMap(reading: SensorReading,out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp = lastTempState.value()
      val tempDiff = (reading.temperature - lastTemp).abs
      if (tempDiff > threshold) {
        out.collect((reading.id, reading.temperature, lastTemp))
        this.lastTempState.update(reading.temperature)
      }
    }
  }


  /**
    * 使用 process function 检测两次的温度大于阈值则告警
    * @param threshold
    */
  class TempChangeAlert2(val threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String,Double,Double)] {
    // 定义一个值状态，保存上一个传感器温度值
    lazy val lastTempState: ValueState[Double] =
      getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, (String,Double,Double)]#Context,
                                out: Collector[(String,Double,Double)]): Unit ={

      val lastTemp = lastTempState.value()

      val diff = (lastTemp - value.temperature).abs
      if(diff > threshold){
        out.collect((ctx.getCurrentKey,lastTemp,value.temperature))
        lastTempState.update(value.temperature)
      }
    }

  }
}
