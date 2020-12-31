package com.hnbian.flink.table

import com.alibaba.fastjson.JSON
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.descriptors.ConnectorDescriptor
import org.apache.flink.table.sources.TableSource


/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/10/20 15:02 
  **/
object TableAPITest {
  def main(args: Array[String]): Unit = {


   /* val env2: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env);

    val dstream: DataStream[String] = env2.socketTextStream("localhost",9999)

    //val dstream: DataStream[String] = env.addSource()

    //val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)



    val ecommerceLogDstream: DataStream[Log] = dstream.map{ jsonString => JSON.parseObject(jsonString,classOf[Log]) }

    ecommerceLogDstream.print()*/


    //val ecommerceLogTable: Table = tableEnv.fromDataStream(ecommerceLogDstream)

    //val table: Table = ecommerceLogTable.select("mid,ch").filter("ch='appstore'")

    //val midchDataStream: DataStream[(String, String)] = table.toAppendStream[(String,String)]

    //midchDataStream.print()
    /*env2.execute()
    env.execute()*/
//    import org.apache.flink.table.api.EnvironmentSettings
//    val fsSettings: EnvironmentSettings = EnvironmentSettings.newInstance.useOldPlanner.inStreamingMode.build
//    //val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    import org.apache.flink.table.api.TableEnvironment
//    val fsTableEnv: TableEnvironment = TableEnvironment.create(fsSettings)
//
//    fsTableEnv.connect(ConnectorDescriptor).createTemporaryTable("tab")
//
//
//
//
//
//    fsTableEnv.execute("")

  }
}

case class Log(id:String,name:String)
