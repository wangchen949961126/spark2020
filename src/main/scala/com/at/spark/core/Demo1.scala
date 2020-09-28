package com.at.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Demo1").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1), ("a", 1)))

    val result: RDD[(String, Int)] = listRDD.reduceByKey((x, y) => (x + y))

    println(result.collect().mkString(","))

    val groupbyRDD: RDD[(String, Iterable[Int])] = listRDD.groupByKey()

  }

}
