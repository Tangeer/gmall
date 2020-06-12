package com.longrise.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.google.common.base.CaseFormat;
import com.longrise.gmall.common.constant.GmallConstant;
import com.longrise.util.MyKafkaSender;

import java.util.List;

/**
 * 拉取canal中的数据，进行业务处理
 */
public class EventHandler {

    public static void handleEvent(String tableName, CanalEntry.EventType eventType, List<RowData> rowDataList){
        if ("order_info".equals(tableName) && eventType == CanalEntry.EventType.INSERT){
            for (RowData rowData : rowDataList){
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();

                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : columnsList) {
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    // 将下划线转成驼峰式
                    String propertiesName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    jsonObject.put(propertiesName, columnValue);
                }

                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonObject.toJSONString());

            }
        }
    }
}
