package com.sparktrarining.spark_project.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
//nc -lk 9999

object NetworkStreaming {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))

    
   
  //DStream[String]
  //   |
  //  RDD[String] ...   
  val inputStream:DStream[String] = ssc.socketTextStream("localhost", 9999)
  
  //Transformations
  val wordDStream = inputStream.flatMap(line => line.split(" "))
  val wordPairDStream = wordDStream.map(word => (word,1))
  val wordCount = wordPairDStream.reduceByKey((a,b) => a+b)
  
  //Action
  wordCount.print()
  
  ssc.start()
  ssc.awaitTermination()
  
  }
  
}