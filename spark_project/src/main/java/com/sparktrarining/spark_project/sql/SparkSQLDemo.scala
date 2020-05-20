package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkSQLDemo {
  
  def main(args:Array[String]):Unit = {
    
    
    //1.6 
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf) // RDD[T]
    val sqlContext  = new SQLContext(sc) //DataFrame/Dataset
    
    //2.x
    //Entry point for Spark SQL
    //SparkSession = SQLContext + SparkContext 
    //DataFrame/Dataset + RDD
    
    //val spark = SparkSession.builder().appName("SparkSQLDemo").master("local[*]").getOrCreate()
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val inputRDD = spark.sparkContext.textFile("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\transaction_dataset.csv")
    
    inputRDD.collect().foreach(println)
    
  }
}