package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object ColumnLevelTransformations {
  
  def main(args: Array[String]): Unit = {

    //Entry point for Spark SQL
    val spark = SparkSession.builder.appName("ColumnLevelTransformations").master("local[*]").getOrCreate()
    import spark.implicits._
    
    
     //StructType(StructField)
 val header = new StructType()
              .add("transaction_id",IntegerType,true) 
              .add("customer_id",IntegerType,true)
              .add("product",StringType,true)
              .add("price",IntegerType,true)
   
  val dfWithHeader = spark.read
                             .schema(header)
                             .csv("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\csv_output\\part-00000-dc0e2117-db0a-4916-a68b-7ba076c235a4-c000.csv")

                             
 
dfWithHeader.show()

val df1 = dfWithHeader.select(
     trim($"transaction_id").as("transaction_id"),
     trim($"customer_id").as("customer_id"),
     lower(trim($"product")).as("product"),
     trim($"price").as("price")
    )

 df1.printSchema()
 df1.show()
 
 
val df2 =  df1.select(
     $"transaction_id",
     $"customer_id",
     when($"product" === "tv", "television").when($"product" === "laptop", "desktop").otherwise($"product").as("product"),
     $"price" 
     )
     
 df2.printSchema()
 df2.show()

 val df3 = df2.withColumn("date_col", lit("2020-5-03"))
 
 val df4 = df3.select(
     
     $"transaction_id",
     $"customer_id",
     $"product",
     upper($"product").as("product_upper"),
     $"price",
     $"date_col"
     
  )
 
 df4.printSchema()
 df4.show()
 
  }
}