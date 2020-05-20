package com.sparktrarining.spark_project.sql

 import org.apache.spark.sql.SparkSession 

/*
 <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_2.11</artifactId>
    <version>2.4.5</version>
    <scope>provided</scope>
</dependency>
 
 */
object HiveReadWrite {
 
  
  def main(args:Array[String]):Unit =  {
    
    
    val spark = SparkSession
                .builder()
                .appName("HiveReadWrite")
                .master("local[*]")
                .enableHiveSupport() //It reads hive metastore
                .getOrCreate()
                
   
    //toDF , toDS            
    import spark.implicits._
 
   //SQL
   val df  = spark.sql("SELECT * FROM training_db.customer_table")
   //val df1 = spark.table("db.customers")
   
   df.printSchema()
   df.show()
   
   //Transformation 
   //df.write.format("hive").saveAsTable("training_db.customer_output_table")
  }
}