package com.sparktrarining.spark_project.sql.usecase

import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import java.util.Properties
import org.apache.spark.sql.Dataset
//Data Wrangling 
//Data Cleansing
object TransformationsDriver {

  case class Employee(val eid: Int, val ename: String, val email: String)
  case class RulesConfig(val col: String, val rule: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("TransformationsDriver").master("local[*]").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("OFF")

    val url = "jdbc:mysql://localhost:3306/empdb"
    val table = "emp"

    val user = "root"
    val password = "Mysql@123"

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)

    //DataFrame = Dataset[Row] = Untyped
    val empDf: Dataset[Row] = spark.read.jdbc(url, table, properties)
    empDf.printSchema()
    empDf.show()

    //Read the config table
    val confDf: Dataset[Row] = spark.read.jdbc(url, "rules_config", properties)
    confDf.printSchema
    confDf.show

    //Convert DataFrame/Dataset[Row] into DataSet[Employee]
    //Functional + SQL
    val empDs: Dataset[Employee] = empDf.as[Employee]
    empDs.printSchema()
    empDs.show()

    val confDs: Dataset[RulesConfig] = confDf.as[RulesConfig]
    confDs.printSchema()
    confDs.show()

    //Dataset[RulesConfig] => Dataset[(col,rule)] map

    val colAndRuleTuple = confDs.collect().map(rc => (rc.col, rc.rule))
    colAndRuleTuple.foreach(println)

    val outputDf = colAndRuleTuple.foldLeft(empDf)((empDf, colAndRule) => {
      val colName = colAndRule._1
      val ruleName = colAndRule._2

      val func = Functions.getFunction(ruleName)
      empDf.transform(func(colName))
    })

    outputDf.printSchema()
    outputDf.show
    
    
    outputDf.write.jdbc(url, "emp_transform_output", properties)
  }
  
  
  

}