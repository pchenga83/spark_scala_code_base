package com.sparktrarining.spark_project.sql.structuredstreaming

import org.apache.spark.sql.SparkSession

object ReadKafka {
  
  def main(args:Array[String]):Unit = {
    
    val spark = SparkSession.builder.appName("ReadKafka").master("local[*]").getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("OFF")
 /* 
    
  +----+----------------------+--------------+---------+------+----------------------+-------------+
|key |value                 |topic         |partition|offset|timestamp             |timestampType|
+----+----------------------+--------------+---------+------+----------------------+-------------+
|null|[70 72 61 6B 61 73 68]|employee_topic|0        |6     |2020-05-13 18:49:02.03|0            |
+----+----------------------+--------------+---------+------+----------------------+-------------+
  */
  //1. Read data from kafka topic  
  val df= spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","employee_topic")
    .load()
  
  //Transformation   
  val ds = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
             .as[(String,String)]
    
 //filter 
 //map
    
    
  //Write  
  ds.writeStream
  .format("console")
  .option("truncate", "false")
  .start()
  .awaitTermination()
  
  }
  
}