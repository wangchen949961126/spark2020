package com.at.spark.stream

import java.util
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau")
    // 开窗 5s
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 获取Kafka流
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream("dau", ssc)

    // 将流中每条数据转换为 StartupLog 类型
    val startuplogDstream: DStream[StartupLog] = inputDstream.map(record => {
      val jsonStr: String = record.value()
      val startupLog: StartupLog = JSON.parseObject(jsonStr, classOf[StartupLog])
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      val dateArr: Array[String] = dateTimeStr.split(" ")
      startupLog.logDate = dateArr(0)
      startupLog.logHour = dateArr(1)
      startupLog
    })
    startuplogDstream.cache()

    // 3   利用用户清单进行过滤 去重  只保留清单中不存在的用户访问记录
    // （批次内的数据可能会有重复，在步骤4中会进行批次内数据的去重）
    val filteredDstream: DStream[StartupLog] = startuplogDstream.transform(rdd => {
      val jedis: Jedis = RedisUtil.getJedisClient //driver //按周期执行
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + dateStr
      // 从redis中读取key为"dau:yyyy-MM-dd"的value
      val dauMidSet: util.Set[String] = jedis.smembers(key)
      jedis.close()

      // 将存储用户mid的Set放入广播变量，广播变量将会有driver分发给所有executor
      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
      println("过滤前：" + rdd.count())
      // 过滤rdd（一批数据）中的mid，剔除广播变量Set中存在mid
      val filteredRDD: RDD[StartupLog] = rdd.filter(startuplog => {
        val dauMidSet: util.Set[String] = dauMidBC.value
        !dauMidSet.contains(startuplog.mid)
      })
      println("过滤后：" + filteredRDD.count())
      filteredRDD
    })

    // 4 批次内进行去重：：按照mid 进行分组，每组取第一个值
    val groupbyMidDstream: DStream[(String, Iterable[StartupLog])] = filteredDstream
      .map(startuplog => (startuplog.mid, startuplog))
      .groupByKey()
    val distictDstream: DStream[StartupLog] = groupbyMidDstream.flatMap { case (mid, startupLogItr) => {
      startupLogItr.toList.take(1)
    }
    }

    // 5 保存今日访问过的用户(mid)清单   -->Redis    1 key类型 ： set    2 key ： dau:2019-xx-xx   3 value : mid
    distictDstream.foreachRDD { rdd =>
      //driver
      rdd.foreachPartition { startuplogItr =>
        val jedis: Jedis = RedisUtil.getJedisClient //executor
        for (startuplog <- startuplogItr) {
          val key = "dau:" + startuplog.logDate
          jedis.sadd(key, startuplog.mid)
          println(startuplog)
        }
        jedis.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()


  }

  case class StartupLog(mid: String,
                        uid: String,
                        appid: String,
                        area: String,
                        os: String,
                        ch: String,
                        logType: String,
                        vs: String,
                        var logDate: String,
                        var logHour: String,
                        var ts: Long
                       ){}
