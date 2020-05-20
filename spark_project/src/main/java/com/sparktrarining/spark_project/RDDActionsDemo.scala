package com.sparktrarining.spark_project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDDActionsDemo {
  
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("WordCountDriver")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
   
    sc.setLogLevel("OFF")
   
    val inputRDD:RDD[String] =  sc.textFile("E:/Big_Data_Training/Batch1/Workspace/spark_project/input_file.txt")
    
    val wordRDD = inputRDD.flatMap(s => s.split(" "))
    
    val pairRDD = wordRDD.map(w => (w,1))
    
    //Stage Bounds or Shuffle operations
    val groupByKeyRDD = pairRDD.groupByKey()
  
    val wordCountRDD = groupByKeyRDD.mapValues(v => v.size)
    
    
    //Lazy Evaluted
    //Actions 
    val result1 = wordCountRDD.collect() //Action or Job0 -> Stages -> Tasks -> Task sends to Executor 
    result1.foreach(println)
    
    val take10 = wordCountRDD.take(10) //Action or Job1 -> Stages -> Tasks -> Task sends to Executor
    take10.foreach(println)
 
    val top10 = wordCountRDD.top(10) //Action or Job2 -> Stages -> Tasks -> Task sends to Executor
    top10.foreach(println)
    
    wordCountRDD.foreach(println) //Action or job3  -> Stages -> Tasks -> Task sends to Executor
    
    val count = wordCountRDD.count()
    println(count)
    
    wordCountRDD.saveAsTextFile("wordcount_text_output")
    wordCountRDD.saveAsObjectFile("wordcount_object_output")
    wordCountRDD.saveAsSequenceFile("wordcount_seq_output")
    
    
    println(wordCountRDD.toDebugString)
  }
  
}