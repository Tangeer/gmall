package com.longrise.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.longrise.gmall.publisher.bean.Option;
import com.longrise.gmall.publisher.bean.OptionGroup;
import com.longrise.gmall.publisher.bean.SaleDetailInfo;
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
        List<Map> totalList = new ArrayList();
        HashMap dauMap = new HashMap();
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

        Map orderAmountMap = new HashMap();
        orderAmountMap.put("id", "orderAmount");
        orderAmountMap.put("name", "新增交易额");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value", orderAmount);
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);

    }


    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam("id") String id, @RequestParam("date") String today){
        HashMap hourMap = new HashMap();
        if ("dau".equals(id)){
            //今天
            Map dateHourTMap = publisherService.getDateHourMap(today);
            // 求昨天分时明细
            String yesterday = getYesterday(today);
            Map dateHourYMap = publisherService.getDateHourMap(yesterday);

            hourMap.put("today", dateHourTMap);
            hourMap.put("yesterday", dateHourYMap);
        }else if ("orderAmount".equals(id)){
            //今天
            Map orderAmountHourTMap = publisherService.getOrderAmountHourMap(today);
            // 求昨天分时明细
            String yesterday = getYesterday(today);
            Map orderAmountHourYMap = publisherService.getOrderAmountHourMap(yesterday);

            hourMap.put("today", orderAmountHourTMap);
            hourMap.put("yesterday", orderAmountHourYMap);
        }
        return JSON.toJSONString(hourMap);
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("keyword") String keyword, @RequestParam("startpage") int startpage, @RequestParam("size") int size){
        Map saleDetailWithAgeAggs = publisherService.getSaleDetail(date, keyword, startpage, size, "user_age");
        Map saleDetailWithGenderAggs = publisherService.getSaleDetail(date, keyword, startpage, size, "user_gender");


        List<Map> sourceList = new ArrayList<>();
        List<OptionGroup> optionGroups = new ArrayList<>();

        int total = (int)saleDetailWithAgeAggs.get("total");
        // 年龄
        System.out.println(saleDetailWithAgeAggs.get("detail"));
        Map<String, Long> ageEsMap = (Map<String, Long>)saleDetailWithAgeAggs.get("group");
        long ageLt20 = 0;
        long age20_30 = 0;
        long ageGte30 = 0;
        for (Map.Entry<String, Long> ageEntry : ageEsMap.entrySet()) {
            Integer age = Integer.parseInt(ageEntry.getKey());
            if (age < 20){
                ageLt20 += ageEntry.getValue();
            }else if (age >= 20 && age < 30){
                age20_30 += ageEntry.getValue();
            }else {
                ageGte30 += ageEntry.getValue();
            }
        }

        List<Option> ageOptionList = new ArrayList<>();
        double ageLt20Rate = Math.round(1000D * ageLt20 / total) / 10.0D;
        double age20_30Rate=  Math.round(1000D* age20_30/total )/10.0D;
        double ageGte30Rate=  Math.round(1000D* ageGte30/  total )/10.0D;
        ageOptionList.add(new Option("20岁以下",ageLt20Rate) );
        ageOptionList.add(new Option("20岁到30岁",age20_30Rate));
        ageOptionList.add(new Option("30岁及30岁以上",ageGte30Rate));

        OptionGroup ageGroup = new OptionGroup("用户年龄占比", ageOptionList);
        optionGroups.add(ageGroup);


        // 性别
        System.out.println(saleDetailWithGenderAggs.get("detail"));
        Map<String,Long> genderEsMap =(Map<String,Long>) saleDetailWithGenderAggs.get("group");
        long maleCount=0;
        long femaleCount=0;

        for (Map.Entry<String, Long> genderEntry : genderEsMap.entrySet()) {
            String gender =  genderEntry.getKey() ;
            if ( gender.equals("M")){
                maleCount+=genderEntry.getValue();
            }else  {
                femaleCount+=genderEntry.getValue();
            }
        }

        List genderOptionList=new ArrayList();
        double maleRate=  Math.round(1000D* maleCount/  total )/10.0D;
        double femaleRate=  Math.round(1000D* femaleCount/  total )/10.0D;

        genderOptionList.add(new Option("男",maleRate));
        genderOptionList.add(new Option("女",femaleRate));

        OptionGroup genderGroup = new OptionGroup("用户性别占比",genderOptionList);
        optionGroups.add(genderGroup);

        SaleDetailInfo saleDetailInfo = new SaleDetailInfo(total, optionGroups, (List<Map>) saleDetailWithAgeAggs.get("detail"));
        return JSON.toJSONString(saleDetailInfo);


    }





}
