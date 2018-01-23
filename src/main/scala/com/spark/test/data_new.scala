package com.spark.test

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object data_new {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.sql.warehouse.dir","file:///E:/XWork/IDEAwork/recomwork/spark-warehouse");
    val sparkConf = new SparkConf().setAppName("data_new").setMaster("local")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val customSchema = StructType(Array(
      StructField("userid", DoubleType, true),
      StructField("gender", DoubleType, true),
      StructField("age", DoubleType, true),
      StructField("viewcount", DoubleType, true),
      StructField("educationno", DoubleType, true),
      StructField("region", DoubleType, true),
      StructField("salary", DoubleType, true),
      StructField("category", DoubleType, true),
      StructField("workyear", DoubleType, true),
      StructField("jobid", DoubleType, true),
      StructField("job_visitcount", DoubleType, true),
      StructField("job_huabu", DoubleType, true),
      StructField("job_fangbu", DoubleType, true),
      StructField("job_doublewage", DoubleType, true),
      StructField("job_annualleave", DoubleType, true),
      StructField("job_examined", DoubleType, true),
      StructField("job_twodayoff", DoubleType, true),
      StructField("job_baochi", DoubleType, true),
      StructField("job_baozhu", DoubleType, true),
      StructField("job_transportation", DoubleType, true),
      StructField("job_insurance", DoubleType, true),
      StructField("job_region", DoubleType, true),
      StructField("job_salary", DoubleType, true),
      StructField("job_education", DoubleType, true),
      StructField("job_category", DoubleType, true),
      StructField("job_type", DoubleType, true),
      StructField("job_workyear", DoubleType, true),
      StructField("job_mark", DoubleType, true),
      StructField("job_worktime", DoubleType, true),
      StructField("label", DoubleType, true),
      StructField("interaction_type", DoubleType, true)
    ))

    val rawdata = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customSchema).load("E:\\traindata\\ga-gbt\\traindata_new_0.csv").drop("data_time")
    rawdata.printSchema()
    println(rawdata.count())

    val ratingFun:(Integer => Integer) = (args:Integer) => {
      if (args == 3){
        1
      }
      else {
        0
      }
    }
    val ratingUDF = udf(ratingFun)

    val data_new = rawdata.withColumn("action_type",ratingUDF(rawdata.col("label"))).drop("interaction_type")
    data_new.printSchema()
    data_new.show()

    val output =new File("E:\\traindata\\ga-gbt\\traindata_new.csv")
    val writer = new PrintWriter(output)
    val result_DF2Rows = data_new.collect()
    var count_1 =0
    var count_0 =0
    result_DF2Rows.foreach( row => {
      writer.write(row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3) +"," + row.get(4) +"," + row.get(5) +"," + row.get(6) +
        "," + row.get(7) +"," + row.get(8) +"," + row.get(9) +"," + row.get(10) +"," + row.get(11) +"," + row.get(12) +"," + row.get(13) +"," + row.get(14) +
        "," + row.get(15) +"," + row.get(16) +"," + row.get(17) +"," + row.get(18) +"," + row.get(19) +"," + row.get(20) +"," + row.get(21) +"," + row.get(22) +
        "," + row.get(23) +"," + row.get(24) +"," + row.get(25) +"," + row.get(26) +"," + row.get(27) +"," + row.get(28) +"," + row.get(29) +"," + row.get(30) + "\n")
      if(row.get(30)==1)
        count_1 = count_1+1
      if(row.get(30)==0)
        count_0 = count_0+1
    })
    println(count_0+","+count_1)
    sc.stop()
  }

}
