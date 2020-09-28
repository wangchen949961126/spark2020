package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object demo03 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("demo03").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val baseRDD: RDD[String] = sc.makeRDD(List("a", "a", "b", "c", "d", "e"))

    val mapRDD: RDD[(String, Int)] = baseRDD.map(x => (x, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    val sortRDD: RDD[(String, Int)] = reduceRDD.sortBy(_._1)

    println(sortRDD.collect.mkString(","))

    sc.stop()
  }


}
