package com.at.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object lx1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("lx1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val fileRDD: RDD[String] = sc.textFile("input/dd.txt")

    // 2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
    val actionRDD: RDD[UserVisitAction] = fileRDD.map(line => {
      val columns: Array[String] = line.split("_")
      UserVisitAction(
        columns(0),
        columns(1).toLong,
        columns(2),
        columns(3).toLong,
        columns(4),
        columns(5),
        columns(6).toLong,
        columns(7).toLong,
        columns(8),
        columns(9),
        columns(10),
        columns(11),
        columns(12).toLong
      )
    })

    val accumulator = new MyAddAcc
    sc.register(accumulator)

    actionRDD.foreach(action => {
      accumulator.add(action)
    })

    // (19,order) -> 23, (10,order) -> 27, (13,click) -> 95
    val aclResult: mutable.HashMap[(String, String), Long] = accumulator.value

    val groupByCID: Map[String, mutable.HashMap[(String, String), Long]] = aclResult.groupBy(kv => kv._1._1)

    val infos: immutable.Iterable[CategoryCountInfo] = groupByCID.map {
      case (cid, map) => {
        CategoryCountInfo(
          cid,
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L))
      }
    }


    val result: List[CategoryCountInfo] = infos.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            if (left.payCount > right.payCount) {
              true
            } else {
              false
            }
          }
          else {
            false
          }
        } else {
          false
        }
      }
    ).take(3)

    result.foreach(println)


  }

  class MyAddAcc extends AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] {

    var map = new mutable.HashMap[(String, String), Long]()

    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] = {
      new MyAddAcc
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: UserVisitAction): Unit = {
      if (v.click_category_id != -1) {
        val key = (v.click_category_id.toString, "click")
        map(key) = map.getOrElse(key, 0L) + 1L
      } else if (v.order_category_ids != "null") {
        val cids: Array[String] = v.order_category_ids.split(",")
        cids.foreach(id => {
          val key = (id, "order")
          map(key) = map.getOrElse(key, 0L) + 1L
        })
      } else if (v.pay_category_ids != "null") {
        val cids: Array[String] = v.pay_category_ids.split(",")
        cids.foreach(id => {
          val key = (id, "pay")
          map(key) = map.getOrElse(key, 0L) + 1L
        })
      }
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]]): Unit = {
      val map1 = map
      val map2 = other.value
      map = map1.foldLeft(map2)(
        (innerMap, kv) => {
          innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2
          innerMap
        }
      )
    }

    override def value: mutable.HashMap[(String, String), Long] = {
      map
    }
  }

  /**
    * 用户访问动作表
    *
    * @param date               用户点击行为的日期
    * @param user_id            用户的ID
    * @param session_id         Session的ID
    * @param page_id            某个页面的ID
    * @param action_time        动作的时间点
    * @param search_keyword     用户搜索的关键词
    * @param click_category_id  某一个商品品类的ID
    * @param click_product_id   某一个商品的ID
    * @param order_category_ids 一次订单中所有品类的ID集合
    * @param order_product_ids  一次订单中所有商品的ID集合
    * @param pay_category_ids   一次支付中所有品类的ID集合
    * @param pay_product_ids    一次支付中所有商品的ID集合
    * @param city_id            城市 id
    */
  case class UserVisitAction(date: String,
                             user_id: Long,
                             session_id: String,
                             page_id: Long,
                             action_time: String,
                             search_keyword: String,
                             click_category_id: Long,
                             click_product_id: Long,
                             order_category_ids: String,
                             order_product_ids: String,
                             pay_category_ids: String,
                             pay_product_ids: String,
                             city_id: Long)

  case class CategoryCountInfo(categoryId: String,
                               clickCount: Long,
                               orderCount: Long,
                               payCount: Long)


}
