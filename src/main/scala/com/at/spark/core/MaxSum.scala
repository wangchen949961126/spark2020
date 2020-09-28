package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MaxSum {

  def main(args: Array[String]): Unit = {

    //val ar = List(List(1, 2), List(3, 4), List(5, 6), List(7, 8))
    //val ints: List[Int] = ar.map(_.max)

    val adList = List(("js", "nj"), ("js", "nj"), ("gx", "gl"))
    //val result: List[((String, String), Int)] = adList.map(x => ((x._1, x._2), 1))

    val result: List[((String, String), Int)] = adList.map {
      case (x, y) => {
        ((x, y), 1)
      }
    }
    println(result)

    //println(result)

  }

}
