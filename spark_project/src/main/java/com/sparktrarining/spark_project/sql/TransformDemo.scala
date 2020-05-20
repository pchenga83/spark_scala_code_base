package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransformDemo {
  
  //DataFrame => DataFrame
  def trimFunc(input:DataFrame):DataFrame = {
    return input.withColumn("ename", trim(col("ename")))
  }
  
  def upperFunc(input:DataFrame): DataFrame = {
    return input.withColumn("ename", upper(col("ename")))
  }
  
  def main(args:Array[String]):Unit ={
    
    val spark = SparkSession.builder.appName("TransformDemo").master("local[*]").getOrCreate()
    //Converts List to DF
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("OFF")
    
    val eid = 10
    val ename = "Prakash"
    val desg  = "Software Devl Mgr"
    //Tuple
    val emp = (10, "Prakash", "Software Devl Mgr","Data Tech")
    
   val empList = List(
                   (1, "Prakash", "Software Devl Mgr","Data Tech"),
                   (2, "A", "Software Devl","Data Tech"),
                   (3, "B", "Software Devl","Data Tech")
    )    
    
    
    val confDf = spark.read.json("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\config.json")
    
    confDf.printSchema()
    confDf.show()
    
    val empDf = empList.toDF("eid","ename","designation","dept")
    
    empDf.printSchema()
    empDf.show()
    
    //empDf => trimFunc => upperFunc
    val finalDf = empDf.transform(trimFunc).transform(upperFunc)
    finalDf.printSchema
    finalDf.show()
  }
  
}