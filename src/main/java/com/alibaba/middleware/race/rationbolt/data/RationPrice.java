package com.alibaba.middleware.race.rationbolt.data;

import java.io.Serializable;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class RationPrice implements Serializable{

    private double pcPrice;

    private double wirlessPrice;

    public RationPrice(double pcPrice, double wirlessPrice) {
        this.pcPrice = pcPrice;
        this.wirlessPrice = wirlessPrice;
    }

    public synchronized double getPcPrice() {
        return pcPrice;
    }

    public void setPcPrice(double pcPrice) {
        this.pcPrice = pcPrice;
    }

    public  synchronized double getWirlessPrice() {
        return wirlessPrice;
    }

    public void setWirlessPrice(double wirlessPrice) {
        this.wirlessPrice = wirlessPrice;
    }

    public synchronized void incrPc(double price)
    {
        this.pcPrice += price;
    }
    public synchronized void incrWireless(double price)
    {
        this.wirlessPrice += price;
    }
}
