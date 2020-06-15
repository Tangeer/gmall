package com.longrise.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDateHourMap(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHourMap(String date);

    public Map getSaleDetail(String date,String keyword ,int startPage, int size, String aggs );

}
