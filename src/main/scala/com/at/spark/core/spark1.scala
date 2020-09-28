package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("spark1").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1), ("a", 1)))

    val result: RDD[(String, Int)] = listRDD.reduceByKey((x, y) => (x + y))

    println(result.collect().mkString(","))


  }

}
