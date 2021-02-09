package com.hnbian.flink.state.keyed

import java.util
import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/2/8 17:26 
  **/
object TestTTL extends App {
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
    .process(new TestTTLValueState)
    .print("TestTTL")
  env.execute()


}

class TestTTLValueState extends KeyedProcessFunction[String, Obj1, String]{

  private val stateTtlConfig: StateTtlConfig = StateTtlConfig
    // 设置状态有效期为 10 秒
    .newBuilder(Time.seconds(10))
    // 在读写数据时都会检查过期数据
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    // 不返回过期的数据值
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build()

  // 定义状态描述符
  val listStateDescriptor = new ListStateDescriptor[Obj1]("objs", Types.of[Obj1])

  // 设置 TTL
  listStateDescriptor.enableTimeToLive(stateTtlConfig)

  private lazy val listState: ListState[Obj1] = getRuntimeContext.getListState(listStateDescriptor)

  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]) = {
    // 获取状态
    val iterable: util.Iterator[Obj1] = listState.get().iterator()

    val list:ListBuffer[Obj1] = new ListBuffer[Obj1]
    while(iterable.hasNext){
      list.append(iterable.next())
    }

    list.append(value)

    import scala.collection.JavaConverters.seqAsJavaListConverter
    val javaList: util.List[Obj1] = list.toList.asJava

    // 更新 状态
    listState.update(javaList)

    out.collect(list.toString())
  }
}
