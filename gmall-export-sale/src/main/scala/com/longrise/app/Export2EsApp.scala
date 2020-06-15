package com.longrise.app

import com.longrise.bean.SaleDetailDaycount
import com.longrise.gmall.common.constant.GmallConstant
import com.longrise.gmall.common.util.MyEsUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Export2EsApp {
  def main(args: Array[String]): Unit = {
    var dt = ""
    if (args.nonEmpty && args(0) != null){
      dt =args(0)
    }else{
      dt = "2020-06-13"
    }

    val conf = new SparkConf().setAppName("export2es").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    session.sql("use gmall")
    val saleDetailRDD: RDD[SaleDetailDaycount] = session.sql("select user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level,cast(order_price as double)," +
      "sku_name,sku_tm_id, sku_category3_id,sku_category2_id,sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name," +
      "spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount,dt " +
      "from dws_sale_detail_daycount where dt='" + dt + "'").as[SaleDetailDaycount].rdd
    saleDetailRDD.foreach(println)
    saleDetailRDD.foreachPartition{saleDetailItr =>
        val saleDetailDaycountList = new ListBuffer[SaleDetailDaycount]()
        for (saleDetail <- saleDetailItr) {
          saleDetailDaycountList += saleDetail
          if (saleDetailDaycountList.size > 0 && saleDetailDaycountList.size % 10 == 0){
            MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE_DETAIL,saleDetailDaycountList.toList)
            saleDetailDaycountList.clear()
          }
        }
        if (saleDetailDaycountList.size>0){
          MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE_DETAIL,saleDetailDaycountList.toList)
        }
    }

  }

}
