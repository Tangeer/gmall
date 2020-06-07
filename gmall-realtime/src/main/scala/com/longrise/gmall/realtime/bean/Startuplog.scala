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
