package com.hnbian.flink.state.keyed

import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
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
object TestMapState extends App {

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
    .process(new TestMapState)
    .print("TestMapState")
  env.execute()
}

class TestMapState extends KeyedProcessFunction[String, Obj1, String]{
  // 定义状态描述符
  private val mapStateDescriptor = new MapStateDescriptor[String,ListBuffer[Obj1]]("objs", Types.of[String], Types.of[ListBuffer[Obj1]])
  private lazy val mapState: MapState[String, ListBuffer[Obj1]] = getRuntimeContext.getMapState(mapStateDescriptor)

  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]): Unit = {
    // 检查当前 key 是否已经存到 map state
    if (mapState.contains(ctx.getCurrentKey)){
      // 获取数据
      val list: ListBuffer[Obj1] = mapState.get(ctx.getCurrentKey)
      if (list.size >=2){
        list.remove(0)
        list.append(value)
        // 添加数据
        mapState.put(ctx.getCurrentKey,list)
      }else{
        list.append(value)
        mapState.put(ctx.getCurrentKey,list)
      }
    }else{
      val list: ListBuffer[Obj1] = ListBuffer(value)
      // 添加数据
      mapState.put(ctx.getCurrentKey,list)
    }
    out.collect(mapState.values().toString)
  }
}