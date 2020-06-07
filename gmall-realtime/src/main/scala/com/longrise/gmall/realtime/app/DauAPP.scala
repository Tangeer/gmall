package com.longrise.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.longrise.gmall.common.constant.GmallConstant
import com.longrise.gmall.common.util.MyEsUtil
import com.longrise.gmall.realtime.bean.Startuplog
import com.longrise.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis



object DauAPP {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

//    inputDStream.foreachRDD(rdd => {
//      println(rdd.map(_.value()).collect().mkString("\n"))
//    })
//    转换处理
    val startuplogStream: DStream[Startuplog] = inputDStream.map(record => {
      val logStr: String = record.value()
      val startuplog: Startuplog = JSON.parseObject(logStr, classOf[Startuplog])
      val date = new Date(startuplog.ts)
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
      val dateArray: Array[String] = dateStr.split(" ")
      startuplog.logDate = dateArray(0)
      startuplog.logHourMinute = dateArray(1)
      startuplog.logHour = dateArray(1).split(":")(0)

      startuplog
    })

    //利用redis进行去重过滤 (5秒批次外的去重)
    val filterDStream: DStream[Startuplog] = startuplogStream.transform { rdd =>
      println("过滤前：" + rdd.count())
      // driver 周期行执行
      val currentDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedis: Jedis = RedisUtil.getJedisClient
      val key: String = "dau:" + currentDate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filterRDD: RDD[Startuplog] = rdd.filter(startuplog => {
        //executor
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startuplog.mid)
      })
      jedis.close()
      println("过滤后：" + filterRDD.count())
      filterRDD
    }

    //去重思路;把相同的mid的数据分成一组 ，每组取第一个(5秒批次内的去重)
    val groupDStream: DStream[(String, Iterable[Startuplog])] = filterDStream.map(startuplog => (startuplog.mid, startuplog)).groupByKey()
    val distinctDStream: DStream[Startuplog] = groupDStream.flatMap {
      case (mid, startuplogItreator) =>
        startuplogItreator.take(1)
    }

    // 保存到redis中
    distinctDStream.foreachRDD{rdd =>
      // driver
      // redis type set
      // key dau:2020-06-03   value: mids
      rdd.foreachPartition{
        startuplogItr => {
          // executor
          val jedis: Jedis = RedisUtil.getJedisClient
          /***注意： startuplogItr迭代器只能迭代一次***/
           val list: List[Startuplog] = startuplogItr.toList
          for (startuplog <- list){
            val key: String = "dau:"+ startuplog.logDate
            val value: String = startuplog.mid
            /*************保存到 redis中************/
            jedis.sadd(key, value)
            println(startuplog)
          }
          /*************保存到 es 中************/
          MyEsUtil.indexBulk(GmallConstant.ES_INDEX_NAME, list)
          jedis.close()
        }
      }

    }




//    //利用redis进行去重过滤
//    startuplogStream.foreachRDD{rdd =>
//      // driver
//      // redis type set
//      // key dau:2020-06-03   value: mids
//      rdd.foreachPartition{
//        startuplogItr => {
//          // executor
//          val jedis: Jedis = RedisUtil.getJedisClient
//          for (startuplog <- startuplogItr){
//            val key: String = "dau:"+ startuplog.logDate
//            val value: String = startuplog.mid
//            jedis.sadd(key, value)
//          }
//          jedis.close()
//        }
//      }
//    }

    ssc.start()

    ssc.awaitTermination()



  }

}
