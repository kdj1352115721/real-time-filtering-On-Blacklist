package com.briup.structStreaming

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import com.briup.structStreaming.BlackListFile.{blackListInfo, userinfoStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}

object BlackListTable extends App {
 // 过滤掉黑名单表中的数据
 // 标记累计
 Logger.getLogger("org").setLevel(Level.WARN)
 val spark: SparkSession = getSpark("BlackTable")
 import spark.implicits._
 //socket输入源读入数据
 val ds: Dataset[(String, Timestamp)] = spark.readStream.format("socket")
  .option("host","127.0.0.1")
  .option("port",9999)
  .option("includeTimeStamp",true)
  .load()
  .as[(String,Timestamp)]       //  value ，timestamp
 // 处理用户访问得到的数据 1
 val userinfoStream: DataFrame = ds.map{
  case (line,times)=>{
   val Array(id,name) = line.split(" ")
   (id.trim.toInt,name,times)   // 将数据整理成一个三元元组返回
  }
 }.toDF("id","name","timestamp")  // 表的列
  println("userinfo处理 ------------")


 //从数据库中的表中读进来数据 2
 val blackListTable: DataFrame = spark.read.format("jdbc")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  //需要显示关闭ssl连接，和时间问题
  .option("url", "jdbc:mysql://127.0.0.1:3306/kang"+"?serverTimezone=GMT%2B8")
  .option("dbtable", "kang.blackList")
  .option("user", "kang")
  .option("password", "kang")
  .load()
  .select($"blackName") // 只拿到自己需要的那一列
  println("数据库表中的数据：黑名单成员：")
//  blackListTable.show()

 //  为什么从jdbc中读入的数据可以进行show ？
 // join,并过滤          留下不在黑名单上的人 1和2  ==》3
 val res: DataFrame = userinfoStream.join(blackListTable,$"name"===$"blackName","left")
  .filter($"blackName".isNull)  //左外连接后，blackName中有值代表该人在黑名单中，需要去除不保留
  .select($"id",$"name",$"timestamp") // 留下那些不在黑名单上的人
 println("join after----------------------- ")

 // 计算   每个用户 一分钟内  的访问次数   使用窗口函数 和水印
 import org.apache.spark.sql.functions._
 import org.apache.spark.sql.expressions._
 val res1: Dataset[(Int, String)] = res.withWatermark("timestamp","30 seconds")
  .groupBy(window($"timestamp","30 seconds"),$"id",$"name") //窗口函数需要时间列，窗口长度，滑动窗口时间，开始时间
   .count()      // 这里是得到每个人在30s内的访问次数  ，一次统计的窗口时间过长，内存太小不满足
   .filter($"count">=10)  // 30S 内 访问次数有点频繁 的用户
   .select($"id",$"name")  //用于保存到数据库中，只取这么两个列的值
   .as[(Int,String)]
   println("每个人在一分钟内的访问次数统计------------------")

  //  Stringming 过程中不能show操作，只能在start 之后
// //将结果写到数据库中：
 val fw: ForeachWriter[(Int, String)] = new ForeachWriter[(Int, String)] {
  var conn :Connection=null;  //var 声明变量必需声明类型，不然不能赋值
  var prep:PreparedStatement = null
  val url="jdbc:mysql://127.0.0.1:3306/kang"+"?serverTimezone=GMT%2B8"
  val user="kang"
  val password="kang"
  //建立连接
  override def open(partitionId: Long, epochId: Long): Boolean = {
   conn = DriverManager.getConnection(url,user,password)
   val sql="insert into blackList(blackId,blackName) values(?,?)"
   prep = conn.prepareStatement(sql)
   true
  }
  // 处理数据
  override def process(value: (Int, String)): Unit = {
   prep.setInt(1,value._1)
   prep.setString(2,value._2)
   prep.execute()  // 执行sql语句之前需要对值赋值
  }
  //关闭连接对象
  override def close(errorOrNull: Throwable): Unit = {
   if(conn!=null) conn.close()
   if(prep!=null) prep.close()
  }
 }
  //可以启动多个流查询
 //将结果输出,写到控制台
  val q: StreamingQuery = res1.writeStream.format("console")
   .outputMode(OutputMode.Complete())
//  .option("checkpointLocation","路径")   // 检查点容错
   .start()

 val q2: StreamingQuery = res1.writeStream.foreach(fw)
  .start()
 println("写入成功！")

 q.awaitTermination()
 q2.awaitTermination()
 spark.close()


}
