package com.hnbian.flink.transform

import com.hnbian.flink.transform.DataStreamTest.setMaxParallelism
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/8/7 22:35 
  **/
object UDFTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    richMap(env)

    setMaxParallelism(env)
    env.execute()
  }

  def richMap(env:StreamExecutionEnvironment): Unit ={
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
    stream.map(new MyRichMapFunction(5)).print("richMap")

//    首先执行 open
//    richMap> 5
//    richMap> 10
//    richMap> 15
//    richMap> 20
//    richMap> 25
//    最后执行 close
  }

  /**
    * 富函数
    * @param i
    * RichMapFunction[Int,Int] 两个类型分别为输入输出类型
    */
  class MyRichMapFunction(i:Int) extends RichMapFunction[Int,Int](){

    override def open(parameters: Configuration): Unit = {
      println("首先执行 open")
      // 可以进行获取数据库连接等初始化操作
    }


    override def map(value: Int): Int = {

//      val context = this.getRuntimeContext
//      println(s"taskName${context.getTaskName}")
      value * i
    }

    override def close(): Unit = {
      println("最后执行 close")
      // 可以进行关闭数据库连接等清理操作
    }
  }

  /**
    * 匿名函数
    * @param env
    */
  def filter2(env:StreamExecutionEnvironment): Unit ={
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
    stream.filter(new FilterFunction[Int] {
      override def filter(value: Int): Boolean = {
        value>3
      }
    }).print("anonymous")
//    anonymous> 4
//    anonymous> 5
  }

  /**
    * 自定义函数
    * @param env
    */
  def  filter1(env:StreamExecutionEnvironment): Unit ={
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
    stream.filter(new MyFilterFunction).print("MyFilterFunction")

    //    MyFilterFunction> 4
    //    MyFilterFunction> 5

  }

}

// 实现自定义 UDF
class MyFilterFunction() extends FilterFunction[Int](){
  override def filter(value: Int): Boolean = {
    value > 3
  }
}

