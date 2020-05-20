package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.udf

object UDFDemo {
  
  //1. Function name : reverse
  //2. Input : String
  //3. Output : String
 
  def reverse(input:String):String = {
    //Implement
    return input.reverse
  }
  

  val i:Int = 10
  //String => String
  val rev1 = (input:String) => input.reverse
  
  
  def main(args:Array[String]):Unit = {
    
    
    val res = reverse("Spark")
    println(res)
    
    val spark = SparkSession.builder.appName("UDFDemo").master("local[*]").getOrCreate()
    //RDD to DF
    //DF to DS 
    import spark.implicits._

    spark.sparkContext.setLogLevel("OFF")

    //spark.read = DataFrameReader
    val transDf = spark.read.option("header", "true").option("inferSchema", "true").csv("E:/Big_Data_Training/Batch1/Workspace/spark_project/transaction_data.csv")

    transDf.printSchema()
    transDf.show()
    
   // register udf into a dataframe
    val revUdf = udf(reverse _ )
    
    val newDf = transDf.select(
        
        $"tras_id",
        $"cust_id",
        $"product",
        revUdf($"product").as("produt_reverse"),
        $"price"
        
        )
    
   newDf.printSchema()
   newDf.show()
   
   
   //SQL 
   
   transDf.createOrReplaceTempView("transaction_table")
   
   //Register a fucn into sparksession 
   spark.udf.register("rev_udf", reverse _)
   
   spark.sql("SELECT tras_id, cust_id, product,rev_udf(product) as product_reverse, length(product) as product_len, price FROM transaction_table").show()
  }
    
  
}