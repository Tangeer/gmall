package com.longrise.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.longrise.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){
        List<Map> totalList = new ArrayList<>();
        HashMap dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");

        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value", dauTotal);
        totalList.add(dauMap);

        Map midMap = new HashMap();
        midMap.put("id", "mid");
        midMap.put("name", "新增设备");

        midMap.put("value", 233);
        totalList.add(midMap);

        return JSON.toJSONString(totalList);

    }


    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam("id") String id, @RequestParam("date") String today){
        if ("dau".equals(id)){
            //今天
            Map dateHourTMap = publisherService.getDateHourMap(today);
            // 求昨天分时明细
            String yesterday = getYesterday(today);
            Map dateHourYMap = publisherService.getDateHourMap(yesterday);

            HashMap hourMap = new HashMap();
            hourMap.put("today", dateHourTMap);
            hourMap.put("yesterday", dateHourYMap);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }

    private String getYesterday(String today) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = "";
        try {
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);
            yesterday = simpleDateFormat.format(yesterdayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }


}
