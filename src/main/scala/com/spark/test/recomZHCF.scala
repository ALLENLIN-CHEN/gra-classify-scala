package com.spark.test

import java.io.PrintWriter

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object recomZHCF {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.sql.warehouse.dir","file:///E:/XWork/IDEAwork/recomwork/spark-warehouse");
    val sparkConf = new SparkConf().setAppName("recomZHCF").setMaster("local")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val conn_str = "jdbc:impala://125.216.242.134:21050/test3"
    val prop = new java.util.Properties
    prop.setProperty("user","hive")
    prop.setProperty("password","")
    prop.setProperty("driver", "com.cloudera.impala.jdbc41.Driver")

    val ratingFun:(Integer => Integer) = (args:Integer) => {
      if (args == 4){
        0
      }
      else {
        args
      }
    }
    val ratingUDF = udf(ratingFun)

    val action_DF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:impala://125.216.242.134:21050/test3", "driver" -> "com.cloudera.impala.jdbc41.Driver", "dbtable" -> "t_interaction", "user" -> "hive", "password" -> "")).load()
      .drop("id").drop("date_time")
    val action_DF1= action_DF.select(
      action_DF.col("user_id").cast(IntegerType).as("userid"),
      action_DF.col("job_id").cast(IntegerType).as("jobid"),
      action_DF.col("interration_type").cast(IntegerType).as("i_type")
    )
    val action_DF2 = action_DF1.withColumn("rating", ratingUDF(action_DF1.col("i_type"))).drop("i_type")
    //action_DF2.printSchema()
    //action_DF2.filter("rating = 0").show()
    //action_DF1.filter("i_type = 4").show()

    def ParseRating(row : Row): Rating = {
      Rating(Integer.parseInt(row.get(0).toString()), Integer.parseInt(row.get(1).toString()), Integer.parseInt(row.get(2).toString()))
    }
    def ParseIntInt(row : Row): (Int, Int) = {
      (Integer.parseInt(row.get(0).toString()), Integer.parseInt(row.get(1).toString()))
    }

    val ratings = action_DF2.rdd.map(ParseRating)
    val test = action_DF2.drop("rating").rdd.map(ParseIntInt)

    val rank = 80
    val numIterations = 6
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    //val predictions = model.predict(test)

    /*val predictions =
      model.predict(test).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }*/
    /*val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)*/

    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
      val usersProducts = data.map { case Rating(user, product, rate) =>
        (user, product)
      }

      val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

      val ratesAndPreds = data.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.join(predictions)

      math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean())
    }
    println(computeRmse(model, ratings))
    
    val users = ratings.map(_.user).distinct()

    val result = users.collect.flatMap { user =>
      model.recommendProducts(user, 50)
    }

    /*val userid = users.collect()
    var recall = 0.0
    var precision = 0.0
    userid.foreach( u => {
      val actualPro = ratings.keyBy(_.user).lookup(u)
      val recPro = model.recommendProducts(u, 20)
      val numArr=new Array[Int](100000)
      var actno = 0
      val recno = recPro.length
      var numHits = 0.0
      actualPro.foreach(ap => {
        //println(ap.product, ap.rating)
        if (ap.rating == 3.0)
          {
            actno = actno + 1
            numArr(ap.product) = 1
          }
      })
      recPro.foreach(rp => {
        //println(rp.product, rp.rating, numArr(rp.product))
        if (numArr(rp.product) == 1)
          {
            numHits = numHits + 1.0
          }
      })
      /*for ((p, i) <- recPro.zipWithIndex) {
        if (actualPro.contains(p)) {
          numHits += 1.0
        }
      }*/
      var temrecall = 0.0
      if (actno == 0) temrecall = 0.0 else temrecall = numHits / actno
      //val temrecall = numHits / actno
      val temprecision = numHits / recno
      //println(numHits, actualPro.size, actno, recno)
      //println("userid", u, "recall", temrecall, "precision", temprecision)
      recall = recall + temrecall
      precision = precision + temprecision
    })

    println("-----------------------------------------------------------------------------------")
    println("recall", recall, "precision", precision, userid.size)
    recall = recall / userid.size
    precision = precision / userid.size
    println("recall", recall, "precision", precision)*/


    val output =new File("result1.csv");
    val writer = new PrintWriter(output)
    result.foreach(r => {
      val tem = "%.2f".format(r.rating)
      writer.write(r.user + "," + r.product + "," + tem + "\n")
    })

    sc.stop()
  }

}
