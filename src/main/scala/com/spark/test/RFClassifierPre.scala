package com.spark.test

import java.io.{File, PrintWriter}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf


object RFClassifierPre {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.sql.warehouse.dir","file:///F:/XWork/IDEAwork/recomwork/spark-warehouse")
    val sparkConf = new SparkConf().setAppName("RFClaPre").setMaster("local")//.set("spark.kryoserializer.buffer.max","64")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rawdata = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawtrainDF").withColumnRenamed("interration_type", "label").drop("data_time")
    rawdata.printSchema()
    //rawdata.sort("userid", "jobid").show()
    println(rawdata.count())

    //rawdata.filter("viewcount is null or age is null or educationno is null or region is null or salary is null or category is null").show()
    //rawdata.filter("job_worktime is null").show()

    val assembler = new VectorAssembler()
      .setInputCols(Array("workyear", "category", "gender", "age", "viewcount", "educationno", "region", "salary", "job_type", "job_visitcount",
        "job_mark", "job_workyear", "job_baochi", "job_baozhu", "job_transportation", "job_twodayoff", "job_annualleave", "job_doublewage",
        "job_fangbu", "job_huabu", "job_insurance", "job_examined", "job_salary", "job_worktime", "job_education", "job_category", "job_region"))
      .setOutputCol("features")

    val trainingData = assembler.transform(rawdata)
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    //    features.printSchema()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainingData)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(2)
      .fit(trainingData)


    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(25)
      .setMaxBins(32).setMaxDepth(12)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("new_prediction")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
    println("-------------------------------Start Training---------------------------------------------------------")
    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)
    //model.write.overwrite().save("hdfs://125.216.242.212:8022/recomdf/RF_model")

    println("-------------------------------Start Prediction---------------------------------------------------------")
    val conn_str = "jdbc:impala://125.216.242.134:21050/test3"
    val prop = new java.util.Properties
    prop.setProperty("user","hive")
    prop.setProperty("password","")
    prop.setProperty("driver", "com.cloudera.impala.jdbc41.Driver")

    val CFResult_DF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_cf_result", "user" -> "hive", "password" -> "")).load()
    CFResult_DF.show()

    val rawUserData = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawUserDF")
    val rawJobData = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawJobDF")

    val testDF = rawUserData.na.drop.join(CFResult_DF, CFResult_DF("user_id") === rawUserData("userid"))
      .join(rawJobData, CFResult_DF("job_id") === rawJobData("jobid"))
      .drop("user_id").drop("job_id")
    //println("-------------------testDF--------------------------")
    //testDF.show()
    //println(testDF.count())
    //println("===================testDF==========================")

    val testData = assembler.transform(testDF)
    //println("-------------------testData--------------------------")
    //testData.show()
    //println("===================testData==========================")
    val predictions = model.transform(testData)
    //predictions.show()
    //predictions.write.mode(SaveMode.Overwrite).save("predictionDF")
    val result_DF = predictions.drop("features").drop("indexedFeatures").drop("rawPrediction").drop("probability").drop("prediction")
      .withColumnRenamed("new_prediction", "prediction")

    //println("-------------------resultDF--------------------------")
    //result_DF.printSchema()
    //result_DF.show()
    println("--------------------------------------------" +result_DF.count() + "-----------------------------------------------")
    //println("===================resultDF==========================")
    result_DF.write.mode(SaveMode.Overwrite).save("hdfs://125.216.242.212:8022/recomdf/resultDF")
    //result_DF.write.option("header", "true").mode(SaveMode.Overwrite).csv("recomZH\\Resource\\result_DF_csv")
    //result_DF.write.format("com.databricks.spark.csv").option("header", "false").mode(SaveMode.Overwrite)
    //  .save("hdfs://125.216.242.212:8022/recomdf/resultCSV")

    /*val conf = new Configuration()
    conf.set("fs.defaultFS","hdfs://125.216.242.212:8022")
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("/user/hive/warehouse/test3.db/t_result_simple/result.txt"))*/
    val ratingFun:(Integer => Integer) = (args:Integer) => {
      if (args == 4){
        0
      }
      else {
        1
      }
    }
    val ratingUDF = udf(ratingFun)

    val output =new File("RFresult2.csv");

    val writer = new PrintWriter(output)
    val result_DF1 = result_DF.select("userid", "jobid", "rating", "prediction")
    val result_DF2 = result_DF1.withColumn("goal", ratingUDF(result_DF1.col("prediction")))
    result_DF2.printSchema()
    result_DF2.show()

    val result_DF2Rows = result_DF2.collect()
    result_DF2Rows.foreach( row => {
      val tem = "%.2f".format(row.get(2))
      writer.write(row.get(0) + "," + row.get(1) + "," + tem + "," + row.get(3) + "," + row.get(4) +"\n")
    })
    sc.stop()
  }

}
