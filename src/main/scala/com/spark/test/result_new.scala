package com.spark.test

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


object result_new {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.sql.warehouse.dir", "file:///F:/XWork/IDEAwork/recomwork/spark-warehouse")
    val sparkConf = new SparkConf().setAppName("ResultClaPreNew").setMaster("local") //.set("spark.kryoserializer.buffer.max","64")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val gbt_schema1 = StructType(Array(
      StructField("user_id1", DoubleType, true),
      StructField("job_id1", DoubleType, true),
      StructField("rating1", DoubleType, true),
      StructField("gbt_prediction1", DoubleType, true)
    ))
    val gbt_result1 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema1).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_1.csv").drop("date_time")


    val gbt_schema2 = StructType(Array(
      StructField("user_id2", DoubleType, true),
      StructField("job_id2", DoubleType, true),
      StructField("rating2", DoubleType, true),
      StructField("gbt_prediction2", DoubleType, true)
    ))
    val gbt_result2 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema2).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_2.csv").drop("date_time")

    val gbt_schema3 = StructType(Array(
      StructField("user_id3", DoubleType, true),
      StructField("job_id3", DoubleType, true),
      StructField("rating3", DoubleType, true),
      StructField("gbt_prediction3", DoubleType, true)
    ))
    val gbt_result3 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema3).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_3.csv").drop("date_time")

    val gbt_schema4 = StructType(Array(
      StructField("user_id4", DoubleType, true),
      StructField("job_id4", DoubleType, true),
      StructField("rating4", DoubleType, true),
      StructField("gbt_prediction4", DoubleType, true)
    ))
    val gbt_result4 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema4).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_4.csv").drop("date_time")

    val gbt_schema5 = StructType(Array(
      StructField("user_id5", DoubleType, true),
      StructField("job_id5", DoubleType, true),
      StructField("rating5", DoubleType, true),
      StructField("gbt_prediction5", DoubleType, true)
    ))
    val gbt_result5 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema5).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_5.csv").drop("date_time")

    val gbt_schema6 = StructType(Array(
      StructField("user_id6", DoubleType, true),
      StructField("job_id6", DoubleType, true),
      StructField("rating6", DoubleType, true),
      StructField("gbt_prediction6", DoubleType, true)
    ))
    val gbt_result6 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema6).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_6.csv").drop("date_time")

    val gbt_schema7 = StructType(Array(
      StructField("user_id7", DoubleType, true),
      StructField("job_id7", DoubleType, true),
      StructField("rating7", DoubleType, true),
      StructField("gbt_prediction7", DoubleType, true)
    ))
    val gbt_result7 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema7).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_7.csv").drop("date_time")

    val gbt_schema8 = StructType(Array(
      StructField("user_id8", DoubleType, true),
      StructField("job_id8", DoubleType, true),
      StructField("rating8", DoubleType, true),
      StructField("gbt_prediction8", DoubleType, true)
    ))
    val gbt_result8 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema8).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_8.csv").drop("date_time")

    val gbt_schema9 = StructType(Array(
      StructField("user_id9", DoubleType, true),
      StructField("job_id9", DoubleType, true),
      StructField("rating9", DoubleType, true),
      StructField("gbt_prediction9", DoubleType, true)
    ))
    val gbt_result9 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema9).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_9.csv").drop("date_time")

    val gbt_schema10 = StructType(Array(
      StructField("user_id10", DoubleType, true),
      StructField("job_id10", DoubleType, true),
      StructField("rating10", DoubleType, true),
      StructField("gbt_prediction10", DoubleType, true)
    ))
    val gbt_result10 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema10).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_10.csv").drop("date_time")

    val gbt_schema11 = StructType(Array(
      StructField("user_id11", DoubleType, true),
      StructField("job_id11", DoubleType, true),
      StructField("rating11", DoubleType, true),
      StructField("gbt_prediction11", DoubleType, true)
    ))
    val gbt_result11 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema11).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_11.csv").drop("date_time")

    val gbt_schema12 = StructType(Array(
      StructField("user_id12", DoubleType, true),
      StructField("job_id12", DoubleType, true),
      StructField("rating12", DoubleType, true),
      StructField("gbt_prediction12", DoubleType, true)
    ))
    val gbt_result12 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema12).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_12.csv").drop("date_time")

    val gbt_schema13 = StructType(Array(
      StructField("user_id13", DoubleType, true),
      StructField("job_id13", DoubleType, true),
      StructField("rating13", DoubleType, true),
      StructField("gbt_prediction13", DoubleType, true)
    ))
    val gbt_result13 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema13).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_13.csv").drop("date_time")

    val gbt_schema14 = StructType(Array(
      StructField("user_id14", DoubleType, true),
      StructField("job_id14", DoubleType, true),
      StructField("rating14", DoubleType, true),
      StructField("gbt_prediction14", DoubleType, true)
    ))
    val gbt_result14 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema14).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_14.csv").drop("date_time")

    val gbt_schema15 = StructType(Array(
      StructField("user_id15", DoubleType, true),
      StructField("job_id15", DoubleType, true),
      StructField("rating15", DoubleType, true),
      StructField("gbt_prediction15", DoubleType, true)
    ))
    val gbt_result15 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema15).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_15.csv").drop("date_time")

    val gbt_schema16 = StructType(Array(
      StructField("user_id16", DoubleType, true),
      StructField("job_id16", DoubleType, true),
      StructField("rating16", DoubleType, true),
      StructField("gbt_prediction16", DoubleType, true)
    ))
    val gbt_result16 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema16).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_16.csv").drop("date_time")

    val gbt_schema17 = StructType(Array(
      StructField("user_id17", DoubleType, true),
      StructField("job_id17", DoubleType, true),
      StructField("rating17", DoubleType, true),
      StructField("gbt_prediction17", DoubleType, true)
    ))
    val gbt_result17 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema17).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_17.csv").drop("date_time")

    val gbt_schema18 = StructType(Array(
      StructField("user_id18", DoubleType, true),
      StructField("job_id18", DoubleType, true),
      StructField("rating18", DoubleType, true),
      StructField("gbt_prediction18", DoubleType, true)
    ))
    val gbt_result18 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema18).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_18.csv").drop("date_time")

    val gbt_schema19 = StructType(Array(
      StructField("user_id19", DoubleType, true),
      StructField("job_id19", DoubleType, true),
      StructField("rating19", DoubleType, true),
      StructField("gbt_prediction19", DoubleType, true)
    ))
    val gbt_result19 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema19).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_19.csv").drop("date_time")

    val gbt_schema20 = StructType(Array(
      StructField("user_id20", DoubleType, true),
      StructField("job_id20", DoubleType, true),
      StructField("rating20", DoubleType, true),
      StructField("gbt_prediction20", DoubleType, true)
    ))
    val gbt_result20 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(gbt_schema20).load("E:\\traindata\\ga-gbt\\result\\GBTresult50_20.csv").drop("date_time")
    gbt_result20.show()

    val all_result1_2 =gbt_result1.na.drop.join(gbt_result2,gbt_result2("user_id2")===gbt_result1("user_id1") and gbt_result2("job_id2")===gbt_result1("job_id1"))
      //.drop("user_id2").drop("job_id2").drop("rating1").drop("rating2")
    //all_result1_2.show()
    val all_result3_4 =gbt_result3.na.drop.join(gbt_result4,gbt_result4("user_id4")===gbt_result3("user_id3") and gbt_result4("job_id4")===gbt_result3("job_id3"))
      //.drop("user_id4").drop("job_id4").drop("rating3").drop("rating4")
    val all_result5_6 =gbt_result5.na.drop.join(gbt_result6,gbt_result6("user_id6")===gbt_result5("user_id5") and gbt_result6("job_id6")===gbt_result5("job_id5"))
      //.drop("user_id6").drop("job_id6").drop("rating5").drop("rating6")
    val all_result7_8 =gbt_result7.na.drop.join(gbt_result8,gbt_result8("user_id8")===gbt_result7("user_id7") and gbt_result8("job_id8")===gbt_result7("job_id7"))
      //.drop("user_id8").drop("job_id8").drop("rating7").drop("rating8")
    val all_result9_10 =gbt_result9.na.drop.join(gbt_result10,gbt_result10("user_id10")===gbt_result9("user_id9") and gbt_result10("job_id10")===gbt_result9("job_id9"))
      //.drop("user_id10").drop("job_id10").drop("rating9").drop("rating10")
    val all_result11_12 =gbt_result11.na.drop.join(gbt_result12,gbt_result12("user_id12")===gbt_result11("user_id11") and gbt_result12("job_id12")===gbt_result11("job_id11"))
      //.drop("user_id12").drop("job_id12").drop("rating11").drop("rating12")
    val all_result13_14 =gbt_result13.na.drop.join(gbt_result14,gbt_result14("user_id14")===gbt_result13("user_id13") and gbt_result14("job_id14")===gbt_result13("job_id13"))
      //.drop("user_id14").drop("job_id14").drop("rating13").drop("rating14")
    val all_result15_16 =gbt_result15.na.drop.join(gbt_result16,gbt_result16("user_id16")===gbt_result15("user_id15") and gbt_result16("job_id16")===gbt_result15("job_id15"))
      //.drop("user_id16").drop("job_id16").drop("rating15").drop("rating16")
    val all_result17_18 =gbt_result17.na.drop.join(gbt_result18,gbt_result18("user_id18")===gbt_result17("user_id17") and gbt_result18("job_id18")===gbt_result17("job_id17"))
      //.drop("user_id18").drop("job_id18").drop("rating17").drop("rating18")
    val all_result19_20 =gbt_result19.na.drop.join(gbt_result20,gbt_result20("user_id20")===gbt_result19("user_id19") and gbt_result20("job_id20")===gbt_result19("job_id19"))
      //.drop("user_id20").drop("job_id20").drop("rating19").drop("rating20")

    val all_result1to4 = all_result1_2.na.drop.join(all_result3_4,all_result3_4("user_id3")===all_result1_2("user_id1") and all_result3_4("job_id3")===all_result1_2("job_id1"))
      //.drop("user_id3").drop("job_id3")
    val all_result5to8 = all_result5_6.na.drop.join(all_result7_8,all_result7_8("user_id7")===all_result5_6("user_id5") and all_result7_8("job_id7")===all_result5_6("job_id5"))
      //.drop("user_id7").drop("job_id8")
    val all_result9to12 = all_result9_10.na.drop.join(all_result11_12,all_result11_12("user_id11")===all_result9_10("user_id9") and all_result11_12("job_id11")===all_result9_10("job_id9"))
      //.drop("user_id11").drop("job_id11")
    val all_result13to16 = all_result13_14.na.drop.join(all_result15_16,all_result15_16("user_id15")===all_result13_14("user_id13") and all_result15_16("job_id15")===all_result13_14("job_id13"))
      //.drop("user_id15").drop("job_id15")
    val all_result17to20 = all_result17_18.na.drop.join(all_result19_20,all_result19_20("user_id19")===all_result17_18("user_id17") and all_result19_20("job_id19")===all_result17_18("job_id17"))
      //.drop("user_id19").drop("job_id19")

    val all_result1to8 = all_result1to4.na.drop.join(all_result5to8,all_result5to8("user_id5")===all_result1to4("user_id1") and all_result5to8("job_id5")===all_result1to4("job_id1"))
      //.drop("user_id5").drop("job_id5")
    val all_result9to16 = all_result9to12.na.drop.join(all_result13to16,all_result13to16("user_id13")===all_result9to12("user_id9") and all_result13to16("job_id13")===all_result9to12("job_id9"))
      //.drop("user_id13").drop("job_id13")

    val all_result1to16 = all_result1to8.na.drop.join(all_result9to16,all_result9to16("user_id9")===all_result1to8("user_id1") and all_result9to16("job_id9")===all_result1to8("job_id1"))
      //.drop("user_id9").drop("job_id9")

    val all_result = all_result1to16.na.drop.join(all_result17to20,all_result17to20("user_id17")===all_result1to16("user_id1") and all_result17to20("job_id17")===all_result1to16("job_id1"))
      //.drop("user_id17").drop("job_id17")
    /*
    val all_result = all_result0.select("user_id1","job_id1","gbt_prediction1","gbt_prediction2","gbt_prediction3","gbt_prediction4","gbt_prediction5"
      ,"gbt_prediction6","gbt_prediction7","gbt_prediction8","gbt_prediction9","gbt_prediction10","gbt_prediction11","gbt_prediction12","gbt_prediction13"
      ,"gbt_prediction14","gbt_prediction15","gbt_prediction16","gbt_prediction17","gbt_prediction18","gbt_prediction19","gbt_prediction20")*/
    //println(all_result.count())
    all_result.printSchema()
    all_result.show()

    /*
    val rf_schema = StructType(Array(
      StructField("user_id", DoubleType, true),
      StructField("job_id", DoubleType, true),
      StructField("rating", DoubleType, true),
      StructField("gbt_prediction", DoubleType, true),
      StructField("rf_prediction", DoubleType, true)
    ))

    val rf_result = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(rf_schema).load("E:\\traindata\\result\\RF_GBT_result100_1.csv").drop("date_time")
    println(rf_result.count())
    rf_result.printSchema()
    rf_result.show()*/

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

    val rawdata = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customSchema).load("E:\\traindata\\ga-gbt\\testdata_new.csv").withColumnRenamed("action_type", "label").drop("data_time").na.drop
    rawdata.printSchema()
    println(rawdata.count())

    val result_DF1 = rawdata.na.drop.join(all_result, all_result("user_id1") === rawdata("userid") and all_result("job_id1")===rawdata("jobid"),"inner")
      .drop("user_id1").drop("job_id1")
      .drop("user_id2").drop("job_id2").drop("rating1").drop("rating2")
      .drop("user_id4").drop("job_id4").drop("rating3").drop("rating4")
      .drop("user_id6").drop("job_id6").drop("rating5").drop("rating6")
      .drop("user_id8").drop("job_id8").drop("rating7").drop("rating8")
      .drop("user_id10").drop("job_id10").drop("rating9").drop("rating10")
      .drop("user_id12").drop("job_id12").drop("rating11").drop("rating12")
      .drop("user_id14").drop("job_id14").drop("rating13").drop("rating14")
      .drop("user_id16").drop("job_id16").drop("rating15").drop("rating16")
      .drop("user_id18").drop("job_id18").drop("rating17").drop("rating18")
      .drop("user_id20").drop("job_id20").drop("rating19").drop("rating20")
      .drop("user_id3").drop("job_id3")
      .drop("user_id7").drop("job_id7")
      .drop("user_id11").drop("job_id11")
      .drop("user_id15").drop("job_id15")
      .drop("user_id19").drop("job_id19")
      .drop("user_id5").drop("job_id5")
      .drop("user_id13").drop("job_id13")
      .drop("user_id9").drop("job_id9")
      .drop("user_id17").drop("job_id17")
      .drop("gender").drop("age").drop("viewcount").drop("educationno").drop("region").drop("salary").drop("category")
      .drop("workyear").drop("job_visitcount").drop("job_huabu").drop("job_fangbu").drop("job_doublewage").drop("job_annualleave").drop("job_examined")
      .drop("job_twodayoff").drop("job_baochi").drop("job_baozhu").drop("job_transportation").drop("job_insurance").drop("job_region").drop("job_salary")
      .drop("job_education").drop("job_category").drop("job_type").drop("job_workyear").drop("job_mark").drop("job_worktime")
    result_DF1.printSchema()
    result_DF1.show()
    val output = new File("E:\\traindata\\ga-gbt\\result\\result_50.csv")
    val writer = new PrintWriter(output)
    val result_DF1Rows = result_DF1.collect()
    result_DF1Rows.foreach(row => {
      writer.write(row.get(0) + "," + row.get(1) + ","+ row.get(2) + "," + row.get(3) + "," + row.get(4) + ","+ row.get(5) +","+row.get(6) + ","+ row.get(7) +","+
        row.get(8) + ","+ row.get(9) +","+row.get(10) + ","+ row.get(11) +","+row.get(12) + ","+ row.get(13) +","+row.get(14) + ","+ row.get(15) +","+
        row.get(16) + ","+ row.get(17) +","+row.get(18) + ","+ row.get(19) +","+row.get(20) + ","+ row.get(21) +","+row.get(22) +"\n")
    })
    /*
    val result_DF = result_DF1.withColumn("prediction",result_DF1.col("gbt_prediction")+result_DF1.col("rf_prediction")).na.drop
    result_DF.printSchema()
    result_DF.show()

    val result_DFRows = result_DF.collect()
    var count_all =0
    */
    var count =0
    /*
    var count_prediction =0
    var count_TP =0*/

    result_DF1Rows.foreach(row => {
      //count_all = count_all +1
      if(row.get(2)==1){
        count = count + 1
      }
      /*
      if(row.get(4)==row.get(5) && row.get(5)==1){
        count_prediction = count_prediction +1
      }
      if(row.get(4)==row.get(5) && row.get(5)==1 && row.get(5)==row.get(2)){
        count_TP = count_TP + 1
      }*/
    })

    //println("总数："+count_all)
    println("实际为真的总数："+count)
    //println("预测为真的总数："+count_prediction)
    //println("预测为真实际为真的总数："+count_TP)*/


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
