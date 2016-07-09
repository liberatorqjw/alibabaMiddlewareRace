package com.alibaba.middleware.race.platformbolt.data;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class OrderMap {

    private double totalprice;
    private String  platform;

    public OrderMap() {
    }

    public OrderMap(double totalprice, String platform) {
        this.totalprice = totalprice;
        this.platform = platform;
    }

    public synchronized double getTotalprice() {
        return totalprice;
    }

    public synchronized void setTotalprice(double totalprice) {
        this.totalprice = totalprice;
    }

    public synchronized String getPlatform() {
        return platform;
    }

    public synchronized void setPlatform(String platform) {
        this.platform = platform;
    }

    public synchronized void descPrice(double price)
    {
        this.totalprice -=price;
    }
}
