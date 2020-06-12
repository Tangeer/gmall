package com.longrise.gmall.realtime.bean

case class Startuplog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      ch: String,
                      Type: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var logHourMinute: String,
                      var ts: Long)




case class OrderInfo(area: String,
                     consignee: String,
                     orderComment: String,
                     var consigneeTel: String,
                     operateTime: String,
                     orderStatus: String,
                     paymentWay: String,
                     userId: String,
                     imgUrl: String,
                     totalAmount: Double,
                     expireTime: String,
                     deliveryAddress: String,
                     createTime: String,
                     trackingNo: String,
                     parentOrderId: String,
                     outTradeNo: String,
                     id: String,
                     tradeBody: String,
                     var createDate: String,
                     var createHour: String,
                     var createHourMinute: String)