package com.sparktrarining.spark_project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FilterDemo1 {
  
   def main(args:Array[String]):Unit = {
     
     val conf = new SparkConf().setAppName("FilterDemo").setMaster("local[*]")
     val sc   = new SparkContext(conf)
     sc.setLogLevel("OFF")
     
     //Read an external sources
     val inputRDD:RDD[String] = sc.textFile("E:/Big_Data_Training/Batch1/Workspace/spark_project/input_file.txt",4)
     
     //inputRDD.foreach(s => println(s))
     
     val len = inputRDD.partitions.length
     
     println(s"No of partitions = ${len}")
     
     /*RDD[
      * "Spark is a distribtuted",
      * "Spark is a distribtuted",
      * "Spark is a distribtuted",
      * "Spark is a distribtuted"
      * 
      * ]
     */
     val filterRDD:RDD[String] = inputRDD.filter(line => line.contains("Scala"))
     
     val result = filterRDD.collect()
     result.foreach(line => println(line))
     
          
     
   }
}