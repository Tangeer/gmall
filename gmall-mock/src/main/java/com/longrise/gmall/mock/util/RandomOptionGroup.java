package com.longrise.gmall.mock.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOptionGroup<T> {

    int totalWeight = 0;
    List<RanOpt> optList = new ArrayList<RanOpt>();

    public RandomOptionGroup(RanOpt<T>... opts){
        for (RanOpt opt : opts){
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }
        }
    }

    public RanOpt<T> getRandomOpt(){
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }





}
