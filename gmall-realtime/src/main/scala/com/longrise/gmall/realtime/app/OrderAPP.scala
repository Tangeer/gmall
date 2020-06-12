package com.longrise.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.longrise.gmall.common.constant.GmallConstant
import com.longrise.gmall.common.util.MyEsUtil
import com.longrise.gmall.realtime.bean.OrderInfo
import com.longrise.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderAPP {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    // 保存到ES
    // 数据脱敏
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, streamingContext)
    val orderInfoDStream: DStream[OrderInfo] = inputDStream.map {
      recond => {
        val jsonStr: String = recond.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        val telSplit: (String, String) = orderInfo.consigneeTel.splitAt(3)
        orderInfo.consigneeTel = telSplit._1 + "********"

        val datetimeArr: Array[String] = orderInfo.createTime.split(" ")
        orderInfo.createDate = datetimeArr(0)
        val timeArr: Array[String] = datetimeArr(1).split(":")
        orderInfo.createHour = timeArr(0)
        orderInfo.createHourMinute = timeArr(0) + ":" + timeArr(1)
        orderInfo
      }
    }

    // 增加一个字段 0 或者 1  标志该订单是否该用户首次下单
    orderInfoDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        orderItr =>
          MyEsUtil.indexBulk(GmallConstant.ES_INDEX_ORDER, orderItr.toList)
      }
    }


    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
