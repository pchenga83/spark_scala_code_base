package com.sparktrarining.spark_project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BroadCastJoin {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BroadCastJoin").setMaster("local[*]")
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
 
 
 finalRDD.foreach(println)
 
 println(s"acc count = ${acc.value}")
  println(s"bad acc count = ${badRecAcc.value}")
 }
 
  
}