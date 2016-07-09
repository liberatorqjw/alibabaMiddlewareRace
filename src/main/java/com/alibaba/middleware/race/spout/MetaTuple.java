package com.alibaba.middleware.race.spout;



import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.model.PaymentMessage;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

public class MetaTuple implements Serializable {

    /**  */
    private static final long serialVersionUID = 2277714452693486955L;


    //protected final MessageQueue mq;



    protected long emitMs;

    protected transient CountDownLatch latch;
    protected transient boolean isSuccess;
    protected PaymentMessage paymentMessage;


    public MetaTuple(PaymentMessage paymentMessage)
    {
        this.paymentMessage = paymentMessage;

    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getEmitMs() {
        return emitMs;
    }

    public void setEmitMs(long emitMs) {
        this.emitMs = emitMs;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setIsSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public PaymentMessage getPaymentMessage() {
        return paymentMessage;
    }

    public void setPaymentMessage(PaymentMessage paymentMessage) {
        this.paymentMessage = paymentMessage;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
