package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//static
object TransactionData  {
  
  def main(args:Array[String]):Unit = {
    
   //Entry for SQL  
   val spark = SparkSession.builder()
                           .appName("TransactionData")
                           .master("local[*]")
                           .getOrCreate()
                           
   import spark.implicits._                        
   
   spark.sparkContext.setLogLevel("OFF")                        
   //Read an external data sources [CSV, TSV, JSON, XML,JDBC, Hive, S3, ADLS]
   val transDF = spark.read
                      .option("header","true")
                      .option("inferSchema","true")
                      .csv("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\transaction_data.csv")                      
  
                       
   //Action
   transDF.printSchema()
   transDF.show()
                     
                      
   //1. SQL
   transDF.createOrReplaceTempView("transaction_table")
   
   //SELECT cust_id, product, price FROM transaction_table WHERE price > 50000 
  val selectDF =  spark.sql("SELECT cust_id, product, price FROM transaction_table WHERE price > 50000")
  //selectDF.printSchema()
  //selectDF.show()
  
  //SELECT SUM(price) AS total_price FROM transaction_table
   val totalPriceDF = spark.sql("SELECT SUM(price) AS total_price FROM transaction_table")
                           
   //totalPriceDF.printSchema()
   //totalPriceDF.show()
   
   //Per Customer total_price
   //SELECT cust_id, SUM(price) AS total_price FROM transaction_table GROUP BY cust_id
   val custWiseTotalDF = spark.sql("SELECT cust_id, SUM(price) AS total_price FROM transaction_table GROUP BY cust_id")
   
   //custWiseTotalDF.printSchema()
   //custWiseTotalDF.show()
   
   
   //Per Product total_price
   //SELECT product, SUM(price) AS total_price FROM transaction_table GROUP BY product
   val productWiseTotalDF = spark.sql("SELECT product, SUM(price) AS total_price FROM transaction_table GROUP BY product")
   //productWiseTotalDF.printSchema()
   //productWiseTotalDF.show()
   
   //2. DataFrame API or DSL (Domain Specific Language)
   //SELECT cust_id, product, price FROM transaction_table WHERE price > 50000
   
   val df1 = transDF.select(
                  $"cust_id",
                  $"product",
                  $"price"
                  ).filter($"price" > 50000)
                  
   df1.printSchema()
   df1.show()
   
   
   val df2 = transDF.select(
                  $"cust_id",
                  $"product",
                  $"price"
                  ).where($"price" > 50000)
                  
   df2.printSchema()
   df2.show()
   
   
   /*val df2 = df1.filter($"price" > 50000)
   df2.show()*/
   
   
   //SELECT SUM(price) AS total_price FROM transaction_table
   //aggregations -- sum, avg, min, max etc 
   //val df3 = transDF.agg(sum($"price").as("total_price"))
   val df3 = transDF.agg(sum($"price").alias("total_price"))
   df3.printSchema()
   df3.show()
   
   
    //Per Customer total_price
   //SELECT cust_id, SUM(price) AS total_price FROM transaction_table GROUP BY cust_id
  
  val df4 = transDF.groupBy($"cust_id").agg(count($"cust_id").as("no.of cust"),sum($"price").as("total_price"))
  df4.printSchema()
  df4.show()
  
  //Per Product total_price
  //SELECT product, SUM(price) AS total_price FROM transaction_table GROUP BY product
  val df5 = transDF.groupBy($"product").agg(sum($"price").as("total_price"))
  df5.printSchema()
  df5.show() 
  
  }
   
  
}