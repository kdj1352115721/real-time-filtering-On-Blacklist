package com.briup.structStreaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
/**
  * 定义最终输出类型
  * 1.实时商品关注度
  * 2.5min内的平均关注度
  * 3.商品ID
  * 4.时间
  * */
//case class ResultGoodData(nowData:Double,avgData:Double,goodID:String,times:Timestamp);
/**
  * 原始数据中1.商品ID 2.时间 3.关注度
  * 状态类
  *   5min内的平均关注度
  *   标记属性 isFlag   true 被标记为过时/无效数据
  * */
//case class ResultGoodState(nowData:Double,avgData:Double,goodID:String,times:Timestamp,isFlag:Boolean)

object Goods {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark=
      SparkSession.builder()
      .master("local[*]")
      .appName("商品关注度")
      .config("spark.sql.streaming.checkpointLocation","Goods-checkPonit")
      .getOrCreate();
    import spark.implicits._

    import org.apache.spark.sql.functions._
    //1.从Kafka的shopInfos读取数据
    val kafkaStream=spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers","node1:9092")
        .option("subscribe","shopInfos")
        .load()
        //.selectExpr("cast(value as string)")
        .select(expr("cast(value as string)"))
        .as[String]

    //2.整理数据格式
    val formatStream=kafkaStream.map{line =>{
        val Array(goodID,browNum,stayTime,isColl,buyNum,times)=
          line.split("::")
      //计算该用户对该商品的关注度
      val rank=browNum.trim.toDouble*0.8+stayTime.trim.toDouble*0.6+isColl.trim.toDouble*1.0+buyNum.trim.toDouble*1.0;
      (goodID,rank,new Timestamp(times.trim.toLong))
    }}.toDF("goodID","rank","times")
      .as[(String,Double,Timestamp)]
    //3.计算每个商品的实时关注度曲线以及连续5分钟不间断的平均关注度
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
          state.setTimeoutDuration("5 minutes");
          //4.返回ResultGoodData对象
          ResultGoodData(resultState.nowData,resultState.avgData,key,resultState.times);
        }
      }
    }

    val result=formatStream.groupByKey(_._1)
      .mapGroupsWithState[ResultGoodState,ResultGoodData](GroupStateTimeout.ProcessingTimeTimeout())(func1)
    //输出到Kafka中
//    val query1=result.writeStream
//        .format("console")
//        .outputMode(OutputMode.Update())
//        .option("truncate",false)
//        .start();
    //result.selectExpr("concat("a1","a2")")
    val query1=result
      .selectExpr("cast(concat('{\"window\":\"',cast(window as String),'\",\"key\":\"',goodID,'\"}') as String) as key","cast(concat('{\"window\":\"',cast(window as string),'\",\"key\":\"',cast(goodID as String),'\",\"avg\":\"',cast(avgData as String),'\",\"now\":\"',cast(nowData as String),'\"}') as String) as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","chenmm")
      .outputMode(OutputMode.Update())
      .start();
    query1.awaitTermination()

    spark.close()
  }
}
