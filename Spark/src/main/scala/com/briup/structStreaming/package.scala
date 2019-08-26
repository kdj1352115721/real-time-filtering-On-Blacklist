package com.briup

import org.apache.spark.sql.SparkSession

package object structStreaming {
 def getSpark(name:String ,master:String = "local[*]") ={
  val sparkSession: SparkSession = SparkSession.builder().appName(name).master(master).getOrCreate()
  sparkSession

 }

}
