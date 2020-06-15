package com.longrise.gmall.publisher.bean;

public class Option {

    // 分类 （男  女）
    String name;

    // 占比
    Double value;

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
