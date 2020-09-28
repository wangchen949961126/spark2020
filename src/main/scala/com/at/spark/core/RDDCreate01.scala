package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDCreate01 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("RDDCreate01").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(List(1,2,3,4))

    val value2: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    
    val value3: RDD[String] = sc.textFile("input")





    sc.stop()

  }

}
