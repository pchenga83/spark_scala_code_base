package com.sparktrarining.spark_project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object OtherOperations {
  
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf().setAppName("OtherOperations").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    //trans_id,product,price,cust_id
    val transRDD:RDD[String] = sc.textFile("E:\\Big_Data_Training\\Batch1\\Workspace\\spark_project\\transaction_dataset.csv")
   
    //(cust_id,cust_name)
    val customerRDD:RDD[String] =  sc.textFile("E:/Big_Data_Training/Batch1/Workspace/spark_project/customer_dataset.csv")
    
    //transRDD.take(10).foreach(println)
    
    //customerRDD.foreach(println)
    
    
    //trans_id,product,price,cust_id  => (cust_id, price)
    //"1,26,9" => (9,26)
    
   val custIdPriceRDD = transRDD.map(line => {
      val arr = line.split(",")
      
      val price = arr(1).toDouble
      val cust_id = arr(2).toInt
      
      (cust_id,price)
    })
    
 custIdPriceRDD.take(10).foreach(println) 
 
 
 //"cust_id, name" => (cust_id,name)
 //"1,Kriste"      => (1, Kriste)

 val custIdNameRDD = customerRDD.map(line => {
   val arr = line.split(",")
   val custId = arr(0).toInt
   val custName = arr(1)
   
   (custId,custName)
 })
 
 //Map[Int,String]
 val custIdNameMap = custIdNameRDD.collectAsMap()
 
 //ReadOnly Shared variable
 val custMapBroadCast = sc.broadcast(custIdNameMap)
 //Mutable shared variable
 val acc = sc.accumulator(0)
 val badRecAcc = sc.accumulator(0)
 
val finalRDD =  custIdPriceRDD.map( t => {
  acc += 1
  val custId = t._1
  val price = t._2
  //(custid,name)
  val map = custMapBroadCast.value
  val name = map.get(custId).getOrElse("NA")
  
  (custId,name,price)
 })
 
 finalRDD.cache() //MEMORY_ONLY (RAM)
 //finalRDD.persist() //MEMORY_ONLY (RAM)

 val MIN_VAL = 1
 val MAX_VAL = 9999
 
 finalRDD.persist(StorageLevel.DISK_ONLY)
 finalRDD.persist(StorageLevel.DISK_ONLY_2) // Replication 2
 
 finalRDD.persist(StorageLevel.MEMORY_ONLY_2) 
 finalRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
 
 
 //Actions
 finalRDD.collect()
 finalRDD.take(10)
 finalRDD.top(10)
 finalRDD.saveAsTextFile("OtherOperations")
 
 finalRDD.unpersist(true) //clear the RDD in Memory or Disk

  }
  
}