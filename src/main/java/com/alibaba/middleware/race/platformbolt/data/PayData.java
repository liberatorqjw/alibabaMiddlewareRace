package com.alibaba.middleware.race.platformbolt.data;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class PayData {

    private long createTime;
    private double curprice;
    private long orderid;

    public PayData(long createTime, double curprice, long orderid) {
        this.createTime = createTime;
        this.curprice = curprice;
        this.orderid = orderid;
    }

    public PayData() {
    }

    public synchronized long getCreateTime() {
        return createTime;
    }

    public synchronized void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public synchronized double getCurprice() {
        return curprice;
    }

    public synchronized void setCurprice(double curprice) {
        this.curprice = curprice;
    }

    public synchronized long getOrderid() {
        return orderid;
    }

    public synchronized void setOrderid(long orderid) {
        this.orderid = orderid;
    }
}
