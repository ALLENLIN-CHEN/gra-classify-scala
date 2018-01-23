package com.spark.test

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Xuh7 on 2017/11/8.
  */
object ziresout {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.sql.warehouse.dir", "file:///F:/XWork/IDEAwork/recomwork/spark-warehouse")
    val sparkConf = new SparkConf().setAppName("RFClaPre").setMaster("local") //.set("spark.kryoserializer.buffer.max","64")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rawdata = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawtrainDF").withColumnRenamed("interration_type", "label").drop("data_time")
    rawdata.printSchema()
    //rawdata.sort("userid", "jobid").show()
    println(rawdata.count())
    rawdata.show()

    val ratingFun:(Integer => Integer) = (args:Integer) => {
      if (args == 4){
        0
      }
      else {
        1
      }
    }
    val ratingUDF = udf(ratingFun)
    val action_DF2 = rawdata.withColumn("rating", ratingUDF(rawdata.col("label")))
    action_DF2.printSchema()
    action_DF2.show()

    action_DF2.write.format("com.databricks.spark.csv").option("header", "false").mode(SaveMode.Overwrite).save("E:\\tarindata0")

    /*val output = new File("zihanres.txt")
    val writer = new PrintWriter(output)
    writer.write("userid,gender,age,viewcount,educationno,region,salary,category,workyear,jobid,job_visitcount,job_huabu,job_fangbu," +
    "job_doublewage,job_annualleave,job_examined,job_twodayoff,job_baochi,job_baozhu,job_transportation,job_insurance,job_region,job_salary," +
    "job_education,job_category,job_type,job_workyear,job_mark,job_worktime,label")
    rawdata.foreach( row => {
      writer.write(row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3) +
        row.get(4) + "," + row.get(5) + "," + row.get(6) + "," + row.get(7) +
        row.get(8) + "," + row.get(9) + "," + row.get(10) + "," + row.get(11) +
        row.get(12) + "," + row.get(13) + "," + row.get(14) + "," + row.get(15) +
        row.get(16) + "," + row.get(17) + "," + row.get(18) + "," + row.get(19) +
        row.get(20) + "," + row.get(21) + "," + row.get(22) + "," + row.get(23) +
        row.get(24) + "," + row.get(25) + "," + row.get(26) + "," + row.get(27) +
        row.get(28) + "," + row.get(29) + "\n")
    })
    writer.flush()*/
  }
}
