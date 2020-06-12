package com.longrise.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.longrise.handler.EventHandler;

import java.net.InetSocketAddress;

public class CanalAPP {

    public static void startConnect(){
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("s166", 11111), "example", "", "");
        while (true){
            canalConnector.connect();
            canalConnector.subscribe("gmall.order_info");
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0){
                System.out.println("5");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONBEGIN) || entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONEND)){
                        continue;
                    }
                    CanalEntry.RowChange rowChange = null;

                    try {
                        // 获取序列化的值
                        rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        EventHandler.handleEvent(entry.getHeader().getTableName(), rowChange.getEventType(), rowChange.getRowDatasList());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
            }


        }
    }


    public static void main(String[] args) {
        CanalAPP.startConnect();
    }


}
