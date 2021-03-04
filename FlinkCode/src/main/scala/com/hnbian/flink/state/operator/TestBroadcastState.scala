package com.hnbian.flink.state.operator

import com.hnbian.flink.common.{Class, Student}
import com.hnbian.flink.process. TestBroadcastProcessFunction
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:01 
  **/
object TestBroadcastState extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[String] = env.socketTextStream("localhost",8888)


  private val StudentStream: DataStream[Student] = stream1
    .map(data => {
      val arr = data.split(",")
      Student(arr(0), arr(1), arr(2))
    })

  val descriptor = new MapStateDescriptor[String,  String]("classInfo", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  val ClassStream: DataStream[Class] = stream2.map(data => {
    val arr = data.split(",")
    Class(arr(0), arr(1))
  })
  val ClassBradoStream: BroadcastStream[Class] = ClassStream.broadcast(descriptor)

  StudentStream
    .connect(ClassBradoStream)
    .process(new CustomBroadcastProcessFunction)
    .print("TestBroadcastProcessFunction")

  env.execute()
}


/**
  * 参数
  * 未广播数据类型
  * 广播数据类型
  * 输出数据类型
  */
class CustomBroadcastProcessFunction extends BroadcastProcessFunction[Student,Class,String]{

  private val mapStateDescriptor = new MapStateDescriptor[String,String]("maps", Types.of[String], Types.of[String])

  override def processElement(value: Student, ctx: BroadcastProcessFunction[Student, Class, String]#ReadOnlyContext, out: Collector[String]): Unit = {

    val classInfo = ctx.getBroadcastState(TestBroadcastProcessFunction.descriptor)

    val className: String = classInfo.get(value.classId)

    out.collect(s"stuId:${value.id}  stuName:${value.name} stuClassName:${className}")
  }

  override def processBroadcastElement(value: Class, ctx: BroadcastProcessFunction[Student, Class, String]#Context, out: Collector[String]): Unit = {

    val classInfo: BroadcastState[String, String] = ctx.getBroadcastState(mapStateDescriptor)
    println("更新状态")
    classInfo.put(value.id,value.name)
  }
}

