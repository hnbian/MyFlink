package com.hnbian.flink.process

import com.hnbian.flink.common.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 11:38 
  **/
object TestSideOutput extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost", 9999)

  private val stream2: DataStream[Student] = stream1
    .map(data => {
      val arr = data.split(",")
      Student(arr(0), arr(1), arr(2))
    })


  val StudentStream: DataStream[Student] = stream2.process(new ProcessFunction[Student,Student] {
    override def processElement(value: Student, ctx: ProcessFunction[Student, Student]#Context, out: Collector[Student]) = {
      lazy val outputTag: OutputTag[Student] = new OutputTag[Student]("class2")
      if (value.classId == "2") {
        ctx.output(outputTag, value)
      }else{
        // 所有数据直接常规输出到主流
        out.collect(value)
      }
    }
  })

  StudentStream
    .getSideOutput(new OutputTag[Student]("class2"))
    .print("outPutTag")

  StudentStream.print("StudentStream")

  env.execute()
}