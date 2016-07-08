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

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public double getCurprice() {
        return curprice;
    }

    public void setCurprice(double curprice) {
        this.curprice = curprice;
    }

    public long getOrderid() {
        return orderid;
    }

    public void setOrderid(long orderid) {
        this.orderid = orderid;
    }
}
