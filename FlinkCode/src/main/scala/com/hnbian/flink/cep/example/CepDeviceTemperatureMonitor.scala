package com.hnbian.flink.cep.example

import java.util
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.mutable

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/4/7 17:56 
  * */
object CepDeviceTemperatureMonitor extends App{

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setParallelism(1)
  // 1. 指定时间类型
  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  environment.setParallelism(1)
  import org.apache.flink.api.scala._

  // 2. 接受数据
  val sourceStream: DataStream[String] = environment.socketTextStream("localhost",9999)

  val deviceStream: KeyedStream[DeviceDetail, String] = sourceStream.map(x => {
    val strings: Array[String] = x.split(",")
    DeviceDetail(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
  }).assignAscendingTimestamps(x =>{format.parse(x.date).getTime})
    .keyBy(x => x.sensorMac)


  // 3. 定义Pattern,指定相关条件和模型序列
  val pattern: Pattern[DeviceDetail, DeviceDetail] =
    Pattern
      .begin[DeviceDetail]("start")
      .where(x =>x.temperature.toInt >= 40)
      .timesOrMore(3)
      .within(Time.minutes(3))

  // 4. 模式检测，将模式应用到流中
  val patternResult: PatternStream[DeviceDetail] = CEP.pattern(deviceStream,pattern)

  // 5. 选取结果
  patternResult.select(new MyPatternResultFunction).print()

  // 6. 启动
  environment.execute("CepDeviceTemperatureMonitor")
}

//自定义PatternSelectFunction
class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail,(String,mutable.Map[String,String])]{
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): (String,mutable.Map[String,String]) = {
    val startDetails: util.List[DeviceDetail] = pattern.get("start")

    //1.通过对偶元组创建map 映射
    val map = mutable.Map[String, String]()
    var deviceMac:String = ""
    for ( i <- 0 until startDetails.size()){
      val deviceDetail = startDetails.get(i)
      deviceMac = deviceDetail.deviceMac
      map.put(deviceDetail.date,deviceDetail.temperature)
    }
    (deviceMac,map)
  }
}

/**
  * 定义温度信息样例类
  * @param sensorMac 传感器设备mac地址
  * @param deviceMac 检测机器mac地址
  * @param temperature 温度
  * @param dampness 湿度
  * @param pressure 气压
  * @param date 数据产生时间
  */
case class DeviceDetail(
                 sensorMac:String,
                 deviceMac:String,
                 temperature:String,
                 dampness:String,
                 pressure:String,
                 date:String)


