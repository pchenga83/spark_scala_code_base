package com.sparktrarining.spark_project.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object WindowDemo {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("StatelessDStream")
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],   //String
      "value.deserializer" -> classOf[StringDeserializer], //String 
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("streaming-topic")
    
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
      
   //Transformations
  //DStream[String]
  //   |
  //  RDD[String] ...   
  val valueDStream:DStream[String] = stream.map(record => record.value())
  val wordDStream = valueDStream.flatMap(line => line.split(" "))
  val wordPairDStream = wordDStream.map(word => (word,1))
  val wordCount = wordPairDStream.reduceByKeyAndWindow((a:Int,b:Int) => (a+b), Seconds(30))
  
  //Action
  wordCount.print()
  
  ssc.start()
  ssc.awaitTermination()
  
  }
  
}