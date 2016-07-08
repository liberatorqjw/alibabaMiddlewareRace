package com.alibaba.middleware.race.platformbolt.timethread;

import com.alibaba.middleware.race.platformbolt.data.OrderMap;
import com.alibaba.middleware.race.platformbolt.data.PayData;
import com.alibaba.middleware.race.platformbolt.data.PlatformData;
import com.alibaba.middleware.race.platformbolt.data.TimePrice;
import com.alibaba.middleware.race.utils.RaceConfig;

import java.util.Iterator;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 遍历pay的队列, 再一次的寻找order是否存在
 * Created by qinjiawei on 16-7-8.
 */
public class PayFindOrder extends TimerTask {

    //private ConcurrentHashMap<Long, OrderMap> OrderDataMap;

    //private LinkedBlockingQueue<PayData> PayAllData;

    public PayFindOrder(ConcurrentHashMap<Long, OrderMap> orderDataMap, LinkedBlockingQueue<PayData> payAllData) {
        //OrderDataMap = orderDataMap;
        //PayAllData = payAllData;
    }

    public PayFindOrder() {
    }

    @Override
    public void run() {

        Iterator<PayData> iters = PlatformData.PayAllData.iterator();

        while (iters.hasNext())
        {
            PayData payData = iters.next();

            //pay还是没有找到order
            if (!PlatformData.OrderDataMap.containsKey(payData.getOrderid()))
                continue;

            //找到order
            else
                {
                    //order的对应总金额减少
                    PlatformData.OrderDataMap.get(payData.getOrderid()).descPrice(payData.getCurprice());
                    if (PlatformData.OrderDataMap.get(payData.getOrderid()).getPlatform().equals(RaceConfig.MqTaobaoTradeTopic))
                    {
                        //
                        if (PlatformData.TaobaoStorgeMap.containsKey(payData.getCreateTime()))
                        {
                            PlatformData.TaobaoStorgeMap.get(payData.getCreateTime()).incrPrice(payData.getCurprice());

                        }
                        else {
                            TimePrice timePrice = new TimePrice(payData.getCurprice());

                            PlatformData.TaobaoStorgeMap.put(payData.getCreateTime(), timePrice);
                        }
                    }
                    else if(PlatformData.OrderDataMap.get(payData.getOrderid()).getPlatform().equals(RaceConfig.MqTmallTradeTopic))
                    {
                        if (PlatformData.TmallStorgeMap.containsKey(payData.getCreateTime()))
                        {
                            PlatformData.TmallStorgeMap.get(payData.getCreateTime()).incrPrice(payData.getCurprice());
                        }
                        else
                        {
                            TimePrice timePrice = new TimePrice(payData.getCurprice());

                            PlatformData.TmallStorgeMap.put(payData.getCreateTime(), timePrice);
                        }
                    }

                    //移除找到的paydata
                    PlatformData.PayAllData.remove(payData);
                }

        }

    }
}
