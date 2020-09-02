package com.hnbian.flink.sink

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020-07-27 23:19 
  **/
object JDBCSink {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("/opt/apache-maven-3.6.0/conf/settings.xml")

    val stream2 = stream.flatMap(_.split(" "))
      .filter(v=>{
        v.length> 5 && v.contains("a")
      })
      .map((_,1))
      .keyBy(0)
      .sum(1)

    stream2.addSink(new JDBCSink())

    env.execute("jdbc sink")
  }

}


class JDBCSink() extends RichSinkFunction[(String,Int)](){

  val url = "jdbc:mysql://**:3306/mydb"
  val uname = "root"
  val pwd = "**"
  // 建立连接
  var conn:Connection = _

  var insertStmt:PreparedStatement = _

  var updateStmt:PreparedStatement = _
  // 预编译

  // 调用连接执行 SQL
  override def invoke(value: (String,Int), context: SinkFunction.Context[_]): Unit = {

    updateStmt.setInt(1,value._2)
    updateStmt.setString(2,value._1)
    updateStmt.execute()
    if(updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1,value._1)
      insertStmt.setInt(2,value._2)
      insertStmt.execute()
    }

  }

  //打开连接 并创建预编译
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    conn = DriverManager.getConnection(url,uname,pwd)
    insertStmt = conn.prepareStatement("insert into tab_jdbc_sink (words,count)values(?,?)")
    updateStmt = conn.prepareStatement("update tab_jdbc_sink set count= ? where words= ? ")
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()

  }
  //

}