package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.SparkSession
import java.util.Properties

object JdbcTest extends App {
  
  val spark = SparkSession.builder.appName("JdbcTest").master("local[*]").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("OFF")
  val prop = new Properties
  
  prop.put("user","root")
  prop.put("password","Mysql@123")
  val df = spark.read.jdbc("jdbc:mysql://localhost:3306/studentdb", "student_table", prop)
  
  df.printSchema()
  df.show()
  
  
  //df.write.jdbc("jdbc:mysql://localhost:3306/studentdb", "student_table_out", prop)
}