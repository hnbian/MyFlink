package com.hnbian.flink.cep.example

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer


/**
  * @Author haonan.bian
  * @Description 使用状态编程的方式 实现ip变更检测
  * @Date 2021/4/7 16:49
  * */
object StateCheckIpChange extends App{

  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  environment.setParallelism(1)

  // 1. 添加 socket 数据源
  val sourceStream: DataStream[String] = environment.socketTextStream("localhost",9999)

  import org.apache.flink.api.scala._
  // 2. 数据处理
  sourceStream.map(
    x =>{
      val strings: Array[String] = x.split(",")
      (strings(1),UserLogin(strings(0),strings(1),strings(2),strings(3)))
    }
  ).keyBy(x => x._1)
    .process(new CheckIpChangeProcessFunction)
    .print()

  environment.execute("checkIpChange")
}


/**
  * 自定义KeyedProcessFunction类
  */
class CheckIpChangeProcessFunction extends KeyedProcessFunction[String,(String,UserLogin),(String,ArrayBuffer[UserLogin])]{

  var valueState: ValueState[UserLogin] = _

  override def open(parameters: Configuration): Unit = {

    val valueStateDescriptor = new ValueStateDescriptor[UserLogin]("changeIp", Types.of[UserLogin])
    valueState = getRuntimeContext.getState(valueStateDescriptor)

  }

  /**
    * 解析用户访问信息
    * @param thisLogin 当前处理的登录数据
    * @param ctx
    * @param out
    */
  override def processElement(
         thisLogin: (String, UserLogin),
         ctx: KeyedProcessFunction[String, (String, UserLogin), (String, ArrayBuffer[UserLogin])]#Context,
         out: Collector[(String, ArrayBuffer[UserLogin])]): Unit = {

    val array = new ArrayBuffer[UserLogin]()
    array.append(thisLogin._2)

    // 取出上次登录 IP
    val prevLogin: UserLogin = valueState.value()

    if (null == prevLogin ){
      valueState.update(thisLogin._2)
    }else if (!prevLogin.ip.equals(thisLogin._2.ip)){
      println("IP 出现变化，重新登录")
      // 更新 IP
      valueState.update(thisLogin._2)
      array.append(prevLogin)
    }
    out.collect((thisLogin._1,array))
  }
}


/**
  * 定义样例类
  * @param ip ip
  * @param username 用户名
  * @param operateUrl 访问地址
  * @param time 访问时间
  */
case class UserLogin(ip:String,username:String,operateUrl:String,time:String)