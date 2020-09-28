package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Demo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val brdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 10, 20, 20, 5, 6))
    val srdd: RDD[Int] = sc.makeRDD(List(6, 7, 8))
    val lrdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6)))

    // map
    val mapRDD: RDD[Int] = brdd.map(x => x * 10)

    // 扁平化 List(List, List) => List()
    val flatMapRDD: RDD[Int] = lrdd.flatMap(x => x)

    // 分组
    val groupByRDD: RDD[(String, Iterable[Int])] = brdd.groupBy(x => if (x % 2 == 1) "1" else "0")

    // 过滤
    val filterRDD: RDD[Int] = brdd.filter(x => x > 10)

    // 去重
    val distinctRDD: RDD[Int] = brdd.distinct()

    // 排序
    val sortByRDD: RDD[Int] = brdd.sortBy(x => x)

    // 合并
    val unionRDD: RDD[Int] = brdd.union(srdd)

    // brdd - srdd
    val subtractRDD: RDD[Int] = brdd.subtract(srdd)

    // 交集
    val intersectionRDD: RDD[Int] = brdd.intersection(srdd)



  }

}
