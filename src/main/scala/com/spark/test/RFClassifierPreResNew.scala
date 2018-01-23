package com.spark.test

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


object RFClassifierPreResNew {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.sql.warehouse.dir","file:///F:/XWork/IDEAwork/recomwork/spark-warehouse")
    val sparkConf = new SparkConf().setAppName("RFClaPreNew").setMaster("local")//.set("spark.kryoserializer.buffer.max","64")
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
      StructField("label", DoubleType, true)))

    //val rawdata = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawtrainDF").withColumnRenamed("interration_type", "label").drop("data_time")
    val rawdata = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customSchema).load("E:\\traindata\\new\\traindata_new.csv").withColumnRenamed("action_type", "label").drop("data_time").na.drop
    rawdata.printSchema()
    //rawdata.sort("userid", "jobid").show()
    println(rawdata.count())

    //rawdata.filter("viewcount is null or age is null or educationno is null or region is null or salary is null or category is null").show()
    //rawdata.filter("job_worktime is null").show()

    val assembler = new VectorAssembler()
      .setInputCols(Array("gender", "age", "viewcount", "educationno","region", "salary", "category", "workyear", "job_visitcount","job_huabu", "job_fangbu", "job_doublewage",
        "job_annualleave", "job_examined", "job_twodayoff", "job_baochi", "job_baozhu", "job_transportation", "job_insurance", "job_region",
        "job_salary", "job_education", "job_category", "job_type", "job_workyear", "job_mark", "job_worktime"))
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
    val gbt_schema = StructType(Array(
      StructField("user_id", DoubleType, true),
      StructField("job_id", DoubleType, true),
      StructField("rating", DoubleType, true),
      StructField("gbt_prediction", DoubleType, true)
    ))

    val CFResult_DF = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema).load("E:\\traindata\\result\\GBTresult100_1.csv").drop("date_time")

    /*val rawUserData = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawUserDF")
    rawUserData.printSchema()
    val rawJobData = sqlContext.read.load("hdfs://125.216.242.212:8022/recomdf/rawJobDF")

    val testDF = rawUserData.na.drop.join(CFResult_DF, CFResult_DF("user_id") === rawUserData("userid"))
      .join(rawJobData, CFResult_DF("job_id") === rawJobData("jobid"))
      .drop("user_id").drop("job_id")*/
    val testSchema = StructType(Array(
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
    val test_data = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(testSchema).load("E:\\traindata\\new\\testdata_new.csv").drop("label").drop("data_time").na.drop
    test_data.printSchema()
    //rawdata.sort("userid", "jobid").show()
    println(test_data.count())

    val testDF = test_data.na.drop.join(CFResult_DF, CFResult_DF("user_id") === test_data("userid") and CFResult_DF("job_id")===test_data("jobid"),"inner")
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
    val result_DF = predictions.drop("features").drop("indexedFeatures").drop("rawPrediction").drop("prediction").drop("probability")
      .withColumnRenamed("new_prediction", "rf_prediction")

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
    /*val ratingFun:(Integer => Integer) = (args:Integer) => {
      if (args == 4){
        0
      }
      else {
        1
      }
    }
    val ratingUDF = udf(ratingFun)*/

    val output =new File("E:\\traindata\\result\\RF_GBT_result100_1.csv")

    val writer = new PrintWriter(output)
    val result_DF2 = result_DF.select("userid", "jobid", "rating","gbt_prediction","rf_prediction")
    result_DF2.show()
    //val result_DF2 = result_DF1.withColumn("goal", ratingUDF(result_DF1.col("prediction")))

    result_DF2.printSchema()
    result_DF2.show()
    println(result_DF2.count())

    val result_DF2Rows = result_DF2.collect()
    result_DF2Rows.foreach( row => {
      val tem = "%.2f".format(row.get(2))
      writer.write(row.get(0) + "," + row.get(1) + "," + tem + "," + row.get(3) +"," + row.get(4) +"\n")
    })
    sc.stop()
  }

}
