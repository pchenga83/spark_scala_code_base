package com.sparktrarining.spark_project

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD

object Test {

  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("WordCountDriver")
    conf.setMaster("local[*]")
    
    val sc = new SparkContext(conf)
   
    val nums:List[Int] = (1 to 100).toList
    
    val rdd:RDD[Int] = sc.parallelize(nums)
    
    println(s"Count = ${rdd.count()}")

      
  }
}