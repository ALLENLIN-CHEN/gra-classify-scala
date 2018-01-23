package com.spark.test

/**
  * Created by Xuh7 on 2017/7/18.
  */

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.mutable

object ClassifierDatatrans {

  def main(args: Array[String]) {
    /*val spark = SparkSession
      .builder
      .appName("RandomForestClassifierExample")
      //.config("spark.sql.warehouse.dir","file:///F:/XWork/IDEAwork/recomwork/spark-warehouse")
      .master("local")
      .getOrCreate()*/
    System.setProperty("spark.sql.warehouse.dir","file:///F:/XWork/IDEAwork/recomwork/spark-warehouse");
    val sparkConf = new SparkConf().setAppName("ClassifierDatatrans").setMaster("local")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val conn_str = "jdbc:impala://125.216.242.134:21050/test3"
    val prop = new java.util.Properties
    prop.setProperty("user","hive")
    prop.setProperty("password","")
    prop.setProperty("driver", "com.cloudera.impala.jdbc41.Driver")

    //获取用户表
    //val user_DF = sqlContext.read.jdbc(conn_str,"t_user",prop)
    val user_DF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_user", "user" -> "hive", "password" -> "")).load()
    user_DF.printSchema()
    user_DF.show()

    //处理用户表category信息，转换后为category
    //val categoryTypeDF = sqlContext.read.jdbc(conn_str,"t_category_type",prop)
    val categoryTypeDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_category_type", "user" -> "hive", "password" -> "")).load()


    val categoryRows = categoryTypeDF.collect()
    //val categoryCols = categoryTypeDF.columns
    var categoryMap = mutable.Map[String, Double]()
    categoryRows.foreach(row => {
      val temId = row.get(0)
      val temName = row.get(1)
      categoryMap += (temName.toString().trim() -> Integer.parseInt(temId.toString()))
    })

    val categoryFun:(String => Double) = (args:String) => {
      if (categoryMap.contains(args)) {
        categoryMap(args)
      }
      else {
        0
      }
    }
    val categoryUDF = udf(categoryFun)

    //处理用户area信息，转换后为region，-1代表其他区域
    //val regionTypeDF = sqlContext.read.jdbc(conn_str,"t_region_type",prop)
    val regionTypeDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_region_type", "user" -> "hive", "password" -> "")).load()


    val regionRows = regionTypeDF.collect()
    var regionMap = mutable.Map[String, Double]()
    regionRows.foreach(row => {
      val temId = row.get(1)
      val temName = row.get(0)
      regionMap += (temName.toString().trim() -> Integer.parseInt(temId.toString()))
    })

    val regionFun:(String => Double) = (args:String) => {
      if ((args == null) || (args.length() < 4)) {
        -1
      }
      else {
        val argslen = args.length()
        if (argslen == 12) {
          0
        }
        else {
          val temargs = args.substring(args.length() - 2, args.length())
          if (regionMap.contains(temargs)) {
            regionMap(temargs)
          }
          else {
            -1
          }
        }
      }
    }
    val regionUDF = udf(regionFun)

    //处理用户salary信息，转换后为salary，只有2、3、5、8K，其余为0
    //val salaryTypeDF = sqlContext.read.jdbc(conn_str,"t_salary_type",prop)
    val salaryTypeDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_salary_type", "user" -> "hive", "password" -> "")).load()


    val salaryRows = salaryTypeDF.collect()
    var salaryMap = mutable.Map[String, Double]()
    salaryRows.foreach(row => {
      val temId = row.get(1)
      val temName = row.get(0)
      salaryMap += (temName.toString().trim() -> Integer.parseInt(temId.toString()))
    })

    val salaryFun:((String, Int) => Double) = (args:String, x1:Int) => {
      if ((args == null) || (args.length() < 2)) {
        0
      }
      else {
        if (args == "\"\"") {
          0
        }
        else {
          val temargs = args(x1) + "K"
          if (salaryMap.contains(temargs)) {
            salaryMap(temargs)
          }
          else {
            0
          }
        }
      }
    }
    val salaryUDF = udf(salaryFun)

    val user_DF2 = user_DF.withColumn("new_category", categoryUDF(user_DF.col("category")))
      .withColumn("region", regionUDF(user_DF.col("area")))
      .withColumn("new_salary", salaryUDF(user_DF.col("salary"), lit(5)))
      .withColumn("new_workyear", user_DF.col("workyear").cast(DoubleType))
      .drop("name").drop("sex_age").drop("target_job").drop("updatetime_view").drop("education").drop("salary").drop("area").drop("workyear")
      .drop("workexperience").drop("edubackground").drop("selfintroduction").drop("category").drop("updatetime").drop("job1").drop("job2")
      .drop("job3").drop("job4").drop("job5").drop("min_salary")
      .withColumnRenamed("sex", "gender")
      .withColumnRenamed("id", "userid")
      .withColumnRenamed("new_category", "category")
      .withColumnRenamed("new_salary", "salary")
      .withColumnRenamed("new_workyear", "workyear")
    user_DF2.printSchema()
    user_DF2.show()
    println(user_DF2.count())
    //user_DF2.write.mode(SaveMode.Overwrite).jdbc(conn_str,"tem_user", prop)
    val user_DF3 = user_DF2.select(
      user_DF2.col("userid").cast(DoubleType).as("userid"),
      user_DF2.col("gender").cast(DoubleType).as("gender"),
      user_DF2.col("age").cast(DoubleType).as("age"),
      user_DF2.col("viewcount").cast(DoubleType).as("viewcount"),
      user_DF2.col("educationno").cast(DoubleType).as("educationno"),
      user_DF2.col("region"),
      user_DF2.col("salary"),
      user_DF2.col("category"),
      user_DF2.col("workyear")
    )
    user_DF3.write.mode(SaveMode.Overwrite).save("hdfs://125.216.242.212:8022/recomdf/rawUserDF")

    //处理job
    //val job_DF = sqlContext.read.jdbc(conn_str,"t_job",prop)
    val job_DF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_job", "user" -> "hive", "password" -> "")).load()
    job_DF.printSchema()
    job_DF.show()

    //处理job_education，基本为空，保险起见处理一下
    //val educationTypeDF = sqlContext.read.jdbc(conn_str,"t_education_type",prop)
    val educationTypeDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_education_type", "user" -> "hive", "password" -> "")).load()


    val educationRows = educationTypeDF.collect()
    var educationMap = mutable.Map[String, Double]()
    educationRows.foreach(row => {
      val temName = row.get(0)
      val temId = row.get(1)
      educationMap += (temName.toString() -> Integer.parseInt(temId.toString()))
    })
    val educationFun:(String => Double) = (args:String) => {
      if (educationMap.contains(args)) {
        educationMap(args)
      }
      else {
        -1
      }
    }
    val educationUDF = udf(educationFun)

    val job_DF2 = job_DF.withColumn("job_salary", salaryUDF(job_DF.col("wage"), lit(0)))
      .withColumn("job_education", educationUDF(job_DF.col("education")))
      .withColumn("job_category", categoryUDF(job_DF.col("category")))
      .drop("title").drop("wage").drop("posttime").drop("company").drop("education").drop("worktime").drop("number").drop("workplace")
      .drop("welfare").drop("description").drop("category").drop("min_salary").drop("postdate")
      //.withColumnRenamed("type", "job_type")
      .withColumnRenamed("id", "jobid")
      .withColumnRenamed("visitcount", "job_visitcount")
      //.withColumnRenamed("mark", "job_mark")
      //.withColumnRenamed("workyear", "job_workyear")
      .withColumnRenamed("baochi", "job_baochi")
      .withColumnRenamed("baozhu", "job_baozhu")
      .withColumnRenamed("transportation", "job_transportation")
      .withColumnRenamed("twodayoff", "job_twodayoff")
      .withColumnRenamed("annualleave", "job_annualleave")
      .withColumnRenamed("doublewage", "job_doublewage")
      .withColumnRenamed("fangbu", "job_fangbu")
      .withColumnRenamed("huabu", "job_huabu")
      .withColumnRenamed("insurance", "job_insurance")
      .withColumnRenamed("examined", "job_examined")
      //.withColumnRenamed("work_time", "job_worktime")
      .withColumnRenamed("region", "job_region")
    job_DF2.printSchema()
    job_DF2.show()
    println(job_DF2.count())

    val job_DF25 = job_DF2.withColumn("job_type", job_DF2.col("type").cast(DoubleType))
      .withColumn("job_workyear", job_DF2.col("workyear").cast(DoubleType))
      .withColumn("job_mark", job_DF2.col("mark").cast(DoubleType))
      .withColumn("job_worktime", job_DF2.col("work_time").cast(DoubleType))
      .drop("type").drop("workyear").drop("mark").drop("work_time")
    val job_DF3 = job_DF25.select(
        job_DF25.col("jobid").cast(DoubleType).as("jobid"),
        job_DF25.col("job_visitcount").cast(DoubleType).as("job_visitcount"),
        job_DF25.col("job_huabu").cast(DoubleType).as("job_huabu"),
        job_DF25.col("job_fangbu").cast(DoubleType).as("job_fangbu"),
        job_DF25.col("job_doublewage").cast(DoubleType).as("job_doublewage"),
        job_DF25.col("job_annualleave").cast(DoubleType).as("job_annualleave"),
        job_DF25.col("job_examined").cast(DoubleType).as("job_examined"),
        job_DF25.col("job_twodayoff").cast(DoubleType).as("job_twodayoff"),
        job_DF25.col("job_baochi").cast(DoubleType).as("job_baochi"),
        job_DF25.col("job_baozhu").cast(DoubleType).as("job_baozhu"),
        job_DF25.col("job_transportation").cast(DoubleType).as("job_transportation"),
        job_DF25.col("job_insurance").cast(DoubleType).as("job_insurance"),
        job_DF25.col("job_region").cast(DoubleType).as("job_region"),
        job_DF25.col("job_salary"),
        job_DF25.col("job_education"),
        job_DF25.col("job_category"),
        job_DF25.col("job_type"),
        job_DF25.col("job_workyear"),
        job_DF25.col("job_mark"),
        job_DF25.col("job_worktime")
      )
    job_DF3.printSchema()
    job_DF3.show()
    job_DF3.write.mode(SaveMode.Overwrite).save("hdfs://125.216.242.212:8022/recomdf/rawJobDF")

    //处理interaction
    //val action_DF2 = sqlContext.read.jdbc(conn_str,"t_interaction",prop).drop("id", "date_time")
    val action_DF2 = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_interaction", "user" -> "hive", "password" -> "")).load()
      .drop("id").drop("date_time")
    action_DF2.printSchema()
    action_DF2.show()

    val rawtrainDF = user_DF3.join(action_DF2, action_DF2("user_id") === user_DF3("userid"))
      .join(job_DF3, action_DF2("job_id") === job_DF3("jobid"))
      .drop("user_id").drop("job_id")

    rawtrainDF.printSchema()
    rawtrainDF.show()

    val rawtrainDF2 = rawtrainDF.withColumn("label", rawtrainDF.col("interration_type").cast(DoubleType))
      .drop("interration_type")
      .withColumnRenamed("label", "interration_type")
    rawtrainDF2.printSchema()
    rawtrainDF2.sort("userid", "jobid").show()
    println(rawtrainDF2.count())

    rawtrainDF2.write.mode(SaveMode.Overwrite).save("hdfs://125.216.242.212:8022/recomdf/rawtrainDF")

    sc.stop()

  }

}
