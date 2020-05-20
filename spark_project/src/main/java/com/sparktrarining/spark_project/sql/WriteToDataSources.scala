package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession

object WriteToDataSources {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("WriteToDataSources").master("local[*]").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("OFF")

    //spark.read = DataFrameReader
    val transDf = spark.read.option("header", "true").option("inferSchema", "true").csv("E:/Big_Data_Training/Batch1/Workspace/spark_project/transaction_data.csv")

    //df.write = DataFrameWriter
    //transDf.write.csv("csv_output")

    /* 1. Columnar format [111,112,113....,1,2,3,4,TV,Laptop,10000,20000]
    2. Compression [Snappy, Gzip ,Bzip2]
    3. Occupify's less space
    4. Process very quickly
   */
    //transDf.write.parquet("parquet_output")

    //RC -- Record Columnar
    //ORC -- Optimized Record Columnar
    /* 1. Columnar format [111,112,113....,1,2,3,4,TV,Laptop,10000,20000]
    2. Compression [Snappy, Gzip ,Bzip2]
    3. Occupify's less space
    4. Process very quickly
    5. Supports Transactional ACID properties */

    //transDf.write.orc("orc_output")
    //transDf.write.mode("append").json("json_output")
    //transDf.write.mode("overwrite").json("json_output")
    
    
    val parDf = spark.read.parquet("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\parquet_output")
    parDf.printSchema()
    parDf.show()
    
    val orcDf = spark.read.orc("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\orc_output")
    orcDf.printSchema()
    orcDf.show()
    
    val jsonDf = spark.read.json("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\json_output")
    jsonDf.printSchema()
    jsonDf.show()
    
    }
}