package com.sparktrarining.spark_project.sql.usecase

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Functions {
  
  def getFunction(rule:String) = {
  
  //transform(DataFrame):DataFrame
  //DataFrame => DataFrame
  //Curry function
  //Partial function
  def trimFunc(colName:String)(input:DataFrame):DataFrame = {
    return input.withColumn(colName, trim(col(colName)))
  }
  
  def lowerFunc(colName:String)(input:DataFrame): DataFrame = {
    return input.withColumn(colName, lower(col(colName)))
  }
  
  def upperFunc(colName:String)(input:DataFrame): DataFrame = {
    return input.withColumn(colName, upper(col(colName)))
  }
  
  def reverseFunc(colName:String)(input:DataFrame): DataFrame = {
    return input.withColumn(colName, reverse(col(colName)))
  }
  
  rule match{
    case "trim"     => trimFunc _
    case "lower"    => lowerFunc _
    case "upper"    => upperFunc _
    case "reverse"  => reverseFunc _
  }
  }
}