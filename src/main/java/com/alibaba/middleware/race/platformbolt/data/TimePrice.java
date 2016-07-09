package com.alibaba.middleware.race.platformbolt.data;

/**
 * 记录整分的交易金额
 * Created by qinjiawei on 16-7-8.
 */
public class TimePrice {

    private double price;

    public TimePrice(double price) {
        this.price = price;
    }

    public synchronized double getPrice() {
        return price;
    }

    public synchronized void setPrice(double price) {
        this.price = price;
    }

    public synchronized void incrPrice(double curprice)
    {
        this.price += curprice;
    }
}
