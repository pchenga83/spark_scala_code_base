package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession

object JoinTwoDataFrames {

  def main(args: Array[String]): Unit = {

   /* val spark = SparkSession.builder.appName("JoinTwoDataFrames").master("local[*]").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("OFF")

    val transDf = spark.read.option("header", "true").option("inferSchema", "true").csv("E:/Big_Data_Training/Batch1/Workspace/spark_project/transaction_data.csv")
    val custDf = spark.read.option("header", "true").option("inferSchema", "true").csv("E:/Big_Data_Training/Batch1/Workspace/spark_project/customer_data.csv")

    transDf.printSchema()
    transDf.show()

    custDf.printSchema()
    custDf.show()

    //1. SQL
    transDf.createOrReplaceTempView("transaction_table")
    custDf.createOrReplaceTempView("customer_table")

    //spark.sql("SELECT * FROM transaction_table").show()
    //spark.sql("SELECT * FROM customer_table").show()

    //INNER OR EQUI
    //SELECT * FROM transaction_table t INNER JOIN customer_table c ON t.cust_id = c.cust_id
    val innerJoinDf = spark.sql("SELECT * FROM transaction_table t INNER JOIN customer_table c ON t.cust_id = c.cust_id")

    //innerJoinDf.printSchema()
    //innerJoinDf.show()

    //LEFT OUTER JOIN = MATCHING RECORDS FROM BOTH THE TABLES + NON MATCHING FROM LEFT TABLE
    //LEFT SIDE ALL RECORDS + RIGHT SIDE ONLY MATCHING
    //SELECT * FROM transaction_table t LEFT OUTER JOIN customer_table c ON t.cust_id = c.cust_id

    val leftOuterJoinDf = spark.sql("SELECT * FROM transaction_table t LEFT OUTER JOIN customer_table c ON t.cust_id = c.cust_id")
    //leftOuterJoinDf.printSchema()
    //leftOuterJoinDf.show()

    //RIGTH OUTER JOIN = MATCHING RECORDS FROM BOTH THE TABLES + NON MATCHING FROM RIGHT TABLE
    //RIGHT SIDE ALL RECORDS + LEFT SIDE ONLY MATCHING

    val rightOuterJoinDf = spark.sql("SELECT * FROM transaction_table t RIGHT OUTER JOIN customer_table c ON t.cust_id = c.cust_id")
    //rightOuterJoinDf.printSchema()
    //rightOuterJoinDf.show()

    //FULL OUTER JOIN = LEFT OUTER  JOIN + RIHGT OUTER JOIN

    val fullOuterJoinDf = spark.sql("SELECT * FROM transaction_table t FULL OUTER JOIN customer_table c ON t.cust_id = c.cust_id")
    //fullOuterJoinDf.printSchema()
    //fullOuterJoinDf.show()

    //2. DataFrame/DSL API
    // Left = transDf,  Right= custDf

    //1. INNER JOIN ===
    //val innerDf = transDf.join(custDf)
    val innerDf = transDf.join(custDf, transDf("cust_id") === custDf("cust_id"), "inner")
    innerDf.printSchema()
    innerDf.show()

    //transDf   custDf
    //      innerDf
    val df1 = innerDf.select(
      $"tras_id".as("transaction_id"),
      transDf.col("cust_id").as("customer_id"),
      $"product",
      $"price",
      $"name".as("customer_name")
      )
      
    df1.printSchema()
    df1.show()
    
    
  //LEFT OUTER JOIN
    
  val leftOuterDf = transDf.join(custDf,transDf("cust_id") === custDf("cust_id"), "left_outer")   
  leftOuterDf.printSchema()
  leftOuterDf.show()  
 
  //RIGHT OUTER JOIN
  val rightOuterDf = transDf.join(custDf,transDf("cust_id") === custDf("cust_id"), "right_outer")   
  rightOuterDf.printSchema()
  rightOuterDf.show()  
  
  //FULL OUTER JOIN
  val fullOuterDf = transDf.join(custDf,transDf("cust_id") === custDf("cust_id"), "full_outer")   
  fullOuterDf.printSchema()
  fullOuterDf.show() 
  
  
  //LEFT SEMI JOIN
  val leftSemiOuterDf = transDf.join(custDf,transDf("cust_id") === custDf("cust_id"), "left_semi")   
  leftSemiOuterDf.printSchema()
  leftSemiOuterDf.show() 
  
   //LEFT ANTI JOIN
  val leftAntiOuterDf = transDf.join(custDf,transDf("cust_id") === custDf("cust_id"), "left_anti")   
  leftAntiOuterDf.printSchema()
  leftAntiOuterDf.show() */
  
    
    val  spark = SparkSession.builder.appName("join_dataframes").master("local[*]").getOrCreate()

    val transaction_df = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\hp\\PycharmProjects\\pyspark\\transaction_data.csv")

    transaction_df.printSchema()
    transaction_df.show()

    val customer_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "C:\\Users\\hp\\PycharmProjects\\pyspark\\customer_data.csv")
    customer_df.printSchema()
    customer_df.show()
  
  
  

  }
}