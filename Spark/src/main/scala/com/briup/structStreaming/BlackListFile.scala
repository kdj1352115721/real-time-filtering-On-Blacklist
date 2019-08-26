package com.briup.structStreaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object BlackListFile extends App {
  // 过滤掉黑名单文件中的用户信息

  //  nc:       id name
 Logger.getLogger("org").setLevel(Level.WARN)
 val spark: SparkSession = getSpark("Black")
 import spark.implicits._
 val ds: Dataset[(String, Timestamp)] = spark.readStream.format("socket")
  .option("host","psuedo")
  .option("port",9999)
  .option("includeTimeStamp",true)
  .load()
  .as[(String,Timestamp)]
  // 用户访问数据
  val userinfoStream: DataFrame = ds.map{
   case (line,_)=>{
    val Array(id,name) = line.split(" ")
    (id.trim.toInt,name)
   }
  }.toDF("id","name")  // 表的列

 // 读取黑名单数据
 val blackListInfo: DataFrame = spark.read.text("Spark/files/BlackList/blackList.txt").toDF("blackName") //并将表中的列起名为

 // join,并过滤得到不在黑名单上的人
 val res: DataFrame = userinfoStream.join(blackListInfo,$"name"===$"blackName","left")
  .filter($"blackName".isNull)
  .select($"id",$"name")

 // 输出到外部系统
 val q: StreamingQuery = res.writeStream.format("console")
  .trigger(Trigger.ProcessingTime("5 seconds"))  //每5s 处理一次数据，吞吐量适合时，吞吐量不合适时，会引起奔溃，是调优的地方
  .start();
 q.awaitTermination()
 spark.close()



}
