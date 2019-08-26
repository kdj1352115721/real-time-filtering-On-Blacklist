package com.briup.structStreaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
/*1.

以上程序自行在各自代码中测试。

结合JavaWeb项目进行数据展示：
1.查看每个商品的实时关注度曲线以及连续5分钟不间断的平均关注度曲线，并保存到Kafka集群中。*/

object GoodsAnalysisByStreaming extends App {
 Logger.getLogger("org").setLevel(Level.WARN)
   val spark: SparkSession = getSpark("GoodsAnalysis");
   import spark.implicits._
 // 从kafka中读取数据
 import org.apache.kafka.clients.CommonClientConfigs
 import org.apache.kafka.clients.consumer.ConsumerConfig
 val kafkaStream: Dataset[String] = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "node1:9092")
  .option("subscribe", "shopInfos")
  .load()
  .selectExpr("CAST(value as String)") // 只拿value这一列数据
  .as[String]

  // 整理数据格式：
  val formatStream: Dataset[(String, Double, Timestamp)] = kafkaStream.map{ line =>{
   val Array(goodID,browNum,stayTime,isColl,buyNum,times)=
    line.split("::")
   //计算该用户对该商品的关注度
   val rank=browNum.trim.toDouble*0.8+stayTime.trim.toDouble*0.6+isColl.trim.toDouble*1.0+buyNum.trim.toDouble*1.0;
   (goodID,rank,new Timestamp(times.trim.toLong))
  }}.toDF("goodID","rank","times")
   .as[(String,Double,Timestamp)]


  // 计算每个商品的实时关注度   以及 5min 内的平均关注度
  val func1 =(key:String,iter:Iterator[(String,Double,Timestamp)]
              ,state:GroupState[ResultGoodState])=>{
   val list=iter.toList;
   //1.判断状态有没有过时
   if(state.hasTimedOut){
    //过时
    state.remove()
    //返回ResultGoodData对象
    ResultGoodData(0.0, 0.0, key, null)
   }else{
    //2.判断状态存不存在
    if(state.exists){
     //存在 代表此数据已经出现过
     //状态对象 存储的上一次的数据信息
     val oldData=state.get;
     //list 存储当前的数据信息
     //当前的关注度
     val nowData=list.map(_._2).sum
     //5min内的关注度
     val avgData=oldData.avgData+nowData
     //最早出现的时间
     val minTimes=oldData.times;
     //构建新的状态对象
     val newState= ResultGoodState(nowData, avgData, key,minTimes , false)
     //更新状态对象
     state.update(newState)
     //返回ResultGoodData对象
     ResultGoodData(newState.nowData, newState.avgData, key, newState.times)
    }else{
     //不存在 第一次出现
     //1.构建状态对象
     //获取该商品数据第一次出现的最早时间
     val minTimes=list.minBy(_._3.toString)._3
     //当前商品的关注度
     val nowData=list.map(_._2).sum
     //5min内商品的关注度
     val avgData=nowData;
     val resultState=ResultGoodState(nowData,avgData,key,minTimes,false);
     //2.更新状态对象
     state.update(resultState);
     //3.设置状态对象的过时时间
     state.setTimeoutDuration("30 seconds");
     //4.返回ResultGoodData对象
     ResultGoodData(resultState.nowData,resultState.avgData,key,resultState.times);
    }
   }
  }

  // 经过goodID进行分组
 val result: Dataset[ResultGoodData] =formatStream.groupByKey(_._1)
  .mapGroupsWithState[ResultGoodState,ResultGoodData](GroupStateTimeout.ProcessingTimeTimeout())(func1)

 // 输出到kafka中：


//  import org.apache.spark.sql.functions._
//  val ds_key: Dataset[((String, String), (String, String))] = df.groupBy(window($"timestamp","30 seconds"),$"key")
//  .select($"window",$"key")
//  .as[((String,String),(String,String))]
//
//  val ds_value: Dataset[((String, String), (String, String), (String, String), (String, String))] = df.groupBy(window($"timestamp","30 seconds"),$"key")
//   .agg(avg("value"),sum("value"))
//   .select($"window",$"key",$"avg",$"sum")
//   .as[((String,String),(String,String),(String,String),(String,String))]
//
//  val ds: DataFrame = ds_key.join(ds_value)
//   .select($"key",$"value")

 //输出到kafka
//  ds.writeStream
//  .format("kafka")
//  .option("kafka.bootstrap.servers","node1:9092,node2:9092:node3:9092")
//  .option("topic","kangUpdates").start()

 /* 注意：    key {}:value{}
 要求输出到Kafka的数据格式必须为：
 key的数据格式为：
             {
               "window":"[2018-06-07 13:02:00, 2018-06-07 13:32:00]",
               "key":"goodsID-3"
             }
           value的数据格式为：
             {
               "window":"[2018-06-07 13:02:00, 2018-06-07 13:32:00]",
               "key":"goodsID-3",
               "avg":"8.346788890",
               "now":"10.35058824"
             }*/
//输出：
 import org.apache.spark.sql.streaming.OutputMode
 val q: StreamingQuery = result.writeStream
  .format("console")
  .outputMode(OutputMode.Update())
  .option("truncate",false)
  .start()

 q.awaitTermination()
 spark.close()



}


case class ResultGoodState(nowData:Double,avgData:Double,goodID:String,times:Timestamp,isFlag:Boolean)
case class ResultGoodData(nowData:Double,avgData:Double,goodID:String,times:Timestamp);