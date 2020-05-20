package com.sparktrarining.spark_project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountDriver {
  
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("WordCountDriver")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
   
    sc.setLogLevel("OFF")
    
    val inputRDD:RDD[String] =  sc.textFile("E:/Big_Data_Training/Batch1/Workspace/spark_project/input_file.txt")
    //inputRDD.foreach(println)
    
    //Transformations
    //1 to many flatMap
    /*1. RDD["Scala is desgin by martin odersky"] => RDD["Scala",
                                                         "is" ] //Word */
    
    val wordRDD:RDD[String] = inputRDD.flatMap(line => line.split(" "))
    //wordRDD.foreach(w => println(w))
    
    
    //2. RDD[String] => RDD[(String,Int)]
    //RDD["Scala"] => RDD[("Scala",1)] 1 -> 1  
  
    val tupleRDD:RDD[(String,Int)] = wordRDD.map(w => (w,1))
    
    //tupleRDD.foreach(println)
    //3. groupByKey
    //RDD[(String,Int)] => RDD[(String,Iterable[Int])]
    //RDD[("Scala",1)] => RDD[("Scala",List(1,1,1,1))]
    val groupByKeyRDD = tupleRDD.groupByKey()
    //groupByKeyRDD.foreach(println)
   
    //val wordCount = groupByKeyRDD.map((t:(String,Iterable[Int])) => (t._1, t._2.size))
    val wordCount  = groupByKeyRDD.mapValues(v => v.size)
    
    //reduceByKey = groupByKey+ reduce
    //val wordCount:RDD[(String,Int)] = tupleRDD.reduceByKey((a,b) => a+b)
     
    //Actions
    //Lazy evalutation
    val count:Long = wordCount.count()
    println(s"count = ${count}")
    
    wordCount.foreach(println)
    
  }
}