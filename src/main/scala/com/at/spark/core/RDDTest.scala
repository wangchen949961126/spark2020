package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {

  def main(args: Array[String]): Unit = {

    println("aa")

    val conf: SparkConf = new SparkConf().setAppName("RDDTest").setMaster("local[*]")

    val sc = new SparkContext(conf)


    val tt: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val pic: RDD[Int] = tt.map(_*2)


    val spec: RDD[Int] = tt.map(_ * 3)

    
    val value3: RDD[String] = sc.textFile("input")


    val xxx: RDD[(Int, Int)] = tt.mapPartitionsWithIndex(
      (index, datas) => (
        datas.map(
          data => (index, data)
        )
        )
    )


    sc.stop()

  }

}
