package com.at.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object sqlDemo02 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("sqlDemo02").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 隐式转换需要导入
    import spark.implicits._

    spark.read.format("jdbc")
      .option("url","")
      .option("user","")
      .option("password","")
      .option("dbtable","T_GROW_EBEAN_STEAL")

    spark.sql(
      """
        |
      """.stripMargin)



    spark.stop()


  }

}

