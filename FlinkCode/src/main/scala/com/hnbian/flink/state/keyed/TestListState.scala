package com.hnbian.flink.state.keyed

import java.util

import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


import scala.collection.mutable.ListBuffer

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:02 
  **/
object TestListState extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  private val value: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  private val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)

  value1
    .process(new TestListState)
    .print("TestListState")
  env.execute()
}

class TestListState extends KeyedProcessFunction[String, Obj1, String]{

  // 定义状态描述符
  val listStateDescriptor = new ListStateDescriptor[Obj1]("objs", Types.of[Obj1])

  private lazy val listState: ListState[Obj1] = getRuntimeContext.getListState(listStateDescriptor)

  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]) = {
    // 获取状态
    val iterable: util.Iterator[Obj1] = listState.get().iterator()

    val list:ListBuffer[Obj1] = new ListBuffer[Obj1]
    while(iterable.hasNext){
      list.append(iterable.next())
    }

    list.append(value)
    // 将新数据加入 listState
    //listState.add(value)

    if (list.size > 2){
      list.remove(0)
    }

    import scala.collection.JavaConverters.seqAsJavaListConverter
    val javaList: util.List[Obj1] = list.toList.asJava

    // 更新 状态
    listState.update(javaList)

    out.collect(list.toString())
  }
}
