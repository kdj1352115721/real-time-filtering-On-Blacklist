package com.briup.structStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.sql.Timestamp
object WindowTest extends App {
 Logger.getLogger("org").setLevel(Level.WARN)
 val spark: SparkSession = getSpark("Test")
 import spark.implicits._
 val ds: Dataset[(String, Timestamp)] = spark.readStream.format("socket")
  .option("host","127.0.0.1")
  .option("port",9999)
  .option("includeTimeStamp",true)
  .load()
  .as[(String,Timestamp)]

 val wordStream: Dataset[(String, Timestamp)] = ds.flatMap{
  case (line,times)=>
     val arr = line.split("[,]")
     arr.map(word => (word,times))
 }.toDF("word","timestamp")
  .as[(String,Timestamp)]

 val res1 = wordStream.groupBy($"word").count()
 // 窗口操作
 import org.apache.spark.sql.functions._
 val res2 = wordStream.groupBy(window($"timestamp","5 seconds"),$"word").count()

 val q: StreamingQuery = res2.writeStream.format("console")
  .option("truncate",false)
  .outputMode("complete")
  .trigger(Trigger.ProcessingTime("5 seconds")) // 经过5m    要根据数据量来设置多少秒处理一次  ，调优
  .start()
 // 默认全部输出，不只是输出20个  truncate flase
 // 追加模式，只会输出一次，输出的契机为 当水印时间（当前窗口的最大事件时间-阈值） 大于这个窗口的最大时间
 // 更新模式，中国数据会输出多次

 q.awaitTermination()
 spark.close()





 }




