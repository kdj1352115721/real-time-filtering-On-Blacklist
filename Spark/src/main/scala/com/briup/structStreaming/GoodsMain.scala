/*
package com.briup.structStreaming

import java.sql.Timestamp

import org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.GroupByStatsRule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.util.StringKeyHashMap
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object GoodsMaine extends  App {
 Logger.getLogger("org").setLevel(Level.WARN)
 val spark = SparkSession.builder().master("local[*]").appName("GoodsMain")
//  .config("spark.sql.streaming.checkepointLocation","")
  .getOrCreate()
 import spark.implicits._

 // 1 从kafka中读取数据
 val kafkaStream: Dataset[String] = spark.readStream
  .format("kafka")
  .option("subscibe","shopInfos")
  .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
  .load()
  .selectExpr("cast(value as string)")
  .as[String]

 //整理数据格式
  val formatStream: Dataset[(String, Double, Timestamp)] = kafkaStream.map{ line =>{
    val Array(goodID,browNum,StayTime,isColl,buyNum,times) = line.split("::")
    (goodID,
     browNum.trim.toInt,
     StayTime.trim.toDouble,
     isColl.trim.toDouble*1.00,
     buyNum.trim.toDouble*1.00,
     new Timestamp(times.trim.toLong))
}
}.toDF("goodID","rank","times")
  .as[(String,Double,Timestamp)]

 import org.apache.spark.sql.streaming.OutputMode
 import org.apache.spark.sql.streaming.GroupStateTimeout
 import org.apache.spark.sql.streaming._
 val fun  = (key:String,iter:Iterator[(String,Double,Timestamp)] ,state:GroupState[ResultGoodState] ) =>{
   val list: List[(String, Double, Timestamp)] = iter.toList
  if(state.hasTimedOut){
   //过时
   state.remove()
   ResultGoodData(0.0,0.0,key,null)
  }
  else{
     // 判断状态存在否
    if(state.exists){
       // 存在 ，代表数据已经出现过
        val oldState = state.get;
        // list  存储当前的数据信息
         //获取当前的关注度
         val nowData: Double = list.map(_._2).sum
        // 获取5min 内的关注度
        // 并取的上次的最早的时间
        val avgData = oldState.avgData+nowData
        val mintimes = oldState.times
        // 构建新的状态对象
     val newState = ResultGoodState(nowData,avgData,key,mintimes,true)
       // 更新状态对象
       state.update(newState)
     ResultGoodData(newState.nowData,newState.avgData,key,newState.times)
    }else{
       // 状态不存在，代表在这个窗口中是第一次出现
       // 先构建一个状态对象，第一次
      // 获取该商品数据第一次出现的最早时间
       val minTimes: Timestamp = list.minBy(_._3.toString)._3

       // 当前商品关注度 , 拿到rank 累加
     val nowData: Double = list.map(_._2).sum
      // 5 Min内的商品关注度
     val avgData = nowData

       val rgoodstate = ResultGoodState(nowData,avgData,key,minTimes,false)

      // 更新状态对象
      state.update(rgoodstate)
     // 设置状态对象的过时时间,只在第一次设置
      state.setTimeoutDuration("5 minutes")
     // 返回一个数据对象
      ResultGoodData(rgoodstate.nowData,rgoodstate.avgData,key,rgoodstate.times)
    }
  }
 }
 formatStream.groupByKey(_._1)
  .flatMapGroupsWithState[ResultGoodData,ResultGoodState](OutputMode.Append(),GroupStateTimeout.ProcessingTimeTimeout())

 val q: StreamingQuery =kafkaStream.writeStream
  .format("console")
  .option("truncate",false)
  .start()

 val q2: StreamingQuery = kafkaStream.writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers","node1:2181,node2:2181,node3:2181")
   .option("topic","kang")
   .outputMode(OutputMode.Update())
   .start()

 q2.awaitTermination()

 q.awaitTermination()
 spark.close()
}

/* 定义最终数据类型
*  1. 实时商品关注度
* 2. 5 min 内的平均关注度
* 3. 商品id
* 4. 时间
*
*
case class ResultGoodData(nowData:Double,avgData:Double,goodId:String,times:Timestamp)
原始数据中，1.商品id  ， 2.时间，  3.关注度
   定义状态类 ，记录一些中间过程
*  1 . 5 min 内的平均关注度
*  2. 标记位  ，  isTimeOut: 如果为true是过时无效的数据
case class ResultGoodState(nowData:Double,avgData:Double,goodId:String,times:Timestamp,isFlag:Boolean)
*/
*/
