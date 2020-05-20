package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types._
/* Scala   SQL Type
 * Byte    ByteType
 * Short   ShortType
 * Int     IntegerType
 * Long    LongType
 *         StructType(StructField)
 */

object RddToDataFrame {
  //Step1 :
  case class Transaction(val trans_id: Int, val cust_id: Int, val product: String, val price: Long)

  def main(args: Array[String]): Unit = {

    //Entry point for Spark SQL
    val spark = SparkSession.builder.appName("RddToDataFrame").master("local[*]").getOrCreate()
    import spark.implicits._

    //RDD
    val sc = spark.sparkContext
    sc.setLogLevel("OFF")

    //Step2
    val inputRDD: RDD[String] = sc.textFile("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\csv_output\\part-00000-dc0e2117-db0a-4916-a68b-7ba076c235a4-c000.csv")
    inputRDD.collect().foreach(println)

    //Step3
    //RDD[String] => RDD[Transaction]
    //RDD["111,1,Laptop,50000"] => RDD[Transaction(111,1,"Laptop",50000)]

    val transRDD: RDD[Transaction] = inputRDD.map(line => {
      val arr: Array[String] = line.split(",")

      val trans_id = arr(0).toInt
      val cust_id = arr(1).toInt
      val product = arr(2)
      val price = arr(3).toLong

      Transaction(trans_id, cust_id, product, price)

    })

    transRDD.foreach(trans => println(trans))

    //Step4
    val transDf = transRDD.toDF()

    transDf.printSchema()
    transDf.show()

    //OR
    
    val df1 = spark.read.option("header","false").option("inferSchema","true").csv("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\csv_output\\part-00000-dc0e2117-db0a-4916-a68b-7ba076c235a4-c000.csv")
    df1.printSchema()
    df1.show()
    
    val finalDf = df1.select(
         $"_c0".as("trans_id"),
         $"_c1".as("cust_id"),
         $"_c2".as("product"),
         $"_c3".as("price")
        );
 
   finalDf.printSchema()
   finalDf.show()
 
 //OR
   
 //StructType(StructField)
 val header = new StructType()
              .add("transaction_id",IntegerType,true) 
              .add("customer_id",IntegerType,true)
              .add("product",StringType,true)
              .add("price",IntegerType,true)
   
  val dfWithHeader = spark.read
                             .schema(header)
                             .csv("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\csv_output\\part-00000-dc0e2117-db0a-4916-a68b-7ba076c235a4-c000.csv")
  dfWithHeader.printSchema()
  dfWithHeader.show()
  }
}