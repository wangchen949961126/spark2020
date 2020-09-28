package com.at.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object sqlDemo01 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("sqlDemo01").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 隐式转换需要导入
    import spark.implicits._

    // val df: DataFrame = spark.read.json("input/json01.txt")
    val df: DataFrame = spark.read.option("header", true).csv("input/cars.csv")

    // sql

    df.createOrReplaceTempView("user")
    val resDF: DataFrame = spark.sql("select * from user")
    resDF.show()

    // resDF.write.format("json").save("output/")

    // DSL
    // df.select("age").show()


    /*
    val users: Seq[User] = Seq(User(1, "zhangsan", 30), User(2, "lisi", 40))
    val ds: Dataset[User] = users.toDS()
    ds.show()
    */

    //val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30)))

    // RDD -> DataFrame
    //val rddToDF: DataFrame = rdd.toDF("id", "name", "age")
    //rddToDF.show()
    // rddToDF.rdd

    // RDD -> Dataset
    //val mapRDD: RDD[User] = rdd.map(t => User(t._1, t._2, t._3))
    //val rddToDS: Dataset[User] = mapRDD.toDS()
    //rddToDS.show()
    // rddToDS.rdd

    // DataFrame -> Dataset
    //val dfToDs: Dataset[User] = rddToDF.as[User]

    // Dataset -> DataFrame
    //val dsToDf: DataFrame = rddToDS.toDF()


    spark.stop()


  }

}

case class User(id: Int,
                name: String,
                age: Int)
