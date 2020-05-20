package com.sparktrarining.spark_project.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object SparkStreaming {
  
  def main(args:Array[String]):Unit = {
    
    //1. Prepare the config object
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    
    //2. Prepare the SparkContext object
    val ssc = new StreamingContext(conf,Seconds(2)) //batch intervals 
    
    println(ssc)
    //ssc.socketStream(hostname, port, converter, storageLevel)
    
  }
}