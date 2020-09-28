package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val a: RDD[String] = sc.textFile("/input")


    sc.textFile("/input")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("/result")

    sc.stop()

  }

}
