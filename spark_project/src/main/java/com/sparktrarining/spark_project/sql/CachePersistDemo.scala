package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object CachePersistDemo {
  
 def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("CachePersistDemo").master("local[*]").getOrCreate()
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

    innerJoinDf.cache() // MEMORY_DISK_ONLY
    //innerJoinDf.persist() //MEMORY_DISK_ONLY
    
    //innerJoinDf.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
        
    innerJoinDf.repartition(10) // Increase/Decrease no of partitions
    innerJoinDf.coalesce(3) // Reduce to no of partitions
    
    //Actions
    innerJoinDf.printSchema()
    innerJoinDf.show()
      
    
   
    
    
 }
}