package com.spark.test

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


object result {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.sql.warehouse.dir", "file:///F:/XWork/IDEAwork/recomwork/spark-warehouse")
    val sparkConf = new SparkConf().setAppName("RFClaPre").setMaster("local") //.set("spark.kryoserializer.buffer.max","64")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rf_schema = StructType(Array(
      StructField("user_id", DoubleType, true),
      StructField("job_id", DoubleType, true),
      StructField("rating", DoubleType, true),
      StructField("gbt_prediction", DoubleType, true),
      StructField("rf_prediction", DoubleType, true)
    ))

    val rf_result = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(rf_schema).load("E:\\traindata\\RF_GBT_result.csv").drop("date_time")
    println(rf_result.count())
    rf_result.printSchema()
    rf_result.show()

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
      StructField("label", DoubleType, true)))

    val rawdata = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customSchema).load("E:\\traindata\\traindata.csv").withColumnRenamed("action_type", "label").drop("data_time")
    rawdata.printSchema()
    println(rawdata.count())

    val result_DF1 = rawdata.na.drop.join(rf_result, rf_result("user_id") === rawdata("userid") and rf_result("job_id")===rawdata("jobid"),"inner")
      .drop("user_id").drop("job_id").drop("gender").drop("age").drop("viewcount").drop("educationno").drop("region").drop("salary").drop("category")
      .drop("workyear").drop("job_visitcount").drop("job_huabu").drop("job_fangbu").drop("job_doublewage").drop("job_annualleave").drop("job_examined")
      .drop("job_twodayoff").drop("job_baochi").drop("job_baozhu").drop("job_transportation").drop("job_insurance").drop("job_region").drop("job_salary")
      .drop("job_education").drop("job_category").drop("job_type").drop("job_workyear").drop("job_mark").drop("job_worktime")
    result_DF1.printSchema()
    result_DF1.show()
    val result_DF = result_DF1.withColumn("prediction",result_DF1.col("gbt_prediction")+result_DF1.col("rf_prediction"))
    result_DF.printSchema()
    result_DF.show()

    val result_DFRows = result_DF.collect()
    var count_all =0
    var count =0
    var count_prediction =0
    var count_TP =0
    result_DFRows.foreach(row => {
      count_all = count_all +1
      if(row.get(2)==1){
        count = count + 1
      }
      if(row.get(4)==row.get(5) && row.get(5)==1){
        count_prediction = count_prediction +1
      }
      if(row.get(4)==row.get(5) && row.get(5)==1 && row.get(5)==row.get(2)){
        count_TP = count_TP + 1
      }
    })

    println(count_all)
    println(count)
    println(count_prediction)
    println(count_TP)


    /*val result_DF2 = result_DF1.withColumn("result",result_DF1.col("gbt_prediction") + result_DF1.col("rf_prediction"))
    result_DF2.printSchema()
    result_DF2.show()

    val output =new File("result_test.csv")
    val writer = new PrintWriter(output)
    val result_DF2Rows = result_DF2.collect()
    result_DF2Rows.foreach( row => {
      val tem = "%.2f".format(row.get(2))
      writer.write(row.get(0) + "," + row.get(1) + "," + tem + "," + row.get(3) + "," + row.get(4) + "," + row.get(5) +"\n")
    })*/
    sc.stop()

  }

}
