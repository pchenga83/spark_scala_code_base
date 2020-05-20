package com.sparktrarining.spark_project.sql

import org.apache.spark.sql.{DataFrame,Row,SparkSession}
import java.util.Properties
import org.apache.spark.sql.Dataset

object ReadDataBase {
  
  case class Employee(val eid:Int, val ename:String, val email: String)
  case class RulesConfig(val col:String, val rule: String)
  
  def main(args:Array[String]):Unit = {
    
    val spark = SparkSession.builder.appName("ReadDataBase").master("local[*]").getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("OFF")
    
    val url      = "jdbc:mysql://localhost:3306/empdb"
    val table    = "emp"
    
    val user     = "root"
    val password = "Mysql@123"
    
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    
    
    //DataFrame = Dataset[Row] = Untyped 
    val empDf:Dataset[Row] = spark.read.jdbc(url, table, properties)
    empDf.printSchema()
    empDf.show()
    
    empDf.collect().foreach((r:Row) => println(r.getInt(0),r.getString(1), r.getString(2)))
    
    
    //Read the config table
    val confDf:Dataset[Row] = spark.read.jdbc(url,"rules_config",properties) 
    confDf.printSchema
    confDf.show
    
    confDf.collect().foreach(r => println(r.getString(0), r.getString(1)))
    
    //Convert DataFrame/Dataset[Row] into DataSet[Employee]
    //Functional + SQL 
    val empDs:Dataset[Employee] =  empDf.as[Employee]
    empDs.printSchema()
    empDs.show()
    
    empDs.collect().foreach(e => println(e.eid, e.ename, e.email))
    
    
    val confDs:Dataset[RulesConfig] = confDf.as[RulesConfig]
    confDs.printSchema()
    confDs.show()
    
    confDs.collect().foreach(r => println(r.col, r.rule))
    
    
    empDs.write.jdbc(url, "emp_output", properties)
    confDs.write.jdbc(url,"conf_output", properties)
    
  }
}
