package com.longrise.gmall.publisher.service.impl;

import com.longrise.gmall.publisher.service.PublisherService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class PublisherServiceImplTest {
    @Autowired
    PublisherService publisherService;
    @Test
    public void getTotal(){
        Integer dauTotal = publisherService.getDauTotal("2020-06-05");
        System.out.println(dauTotal);
    }

    @Test
    public void getDateHourMap(){
        Map dateHourMap = publisherService.getDateHourMap("2020-06-05");
        System.out.println(dateHourMap.toString());
    }

    @Test
    public void getOrderAmount(){
        Double orderAmount = publisherService.getOrderAmount("2020-06-05");
        System.out.println(orderAmount);
    }

    @Test
    public void getOrderAmountHourMap(){
        Map orderAmountHourMap = publisherService.getOrderAmountHourMap("2020-06-05");
        System.out.println(orderAmountHourMap.toString());
    }

}