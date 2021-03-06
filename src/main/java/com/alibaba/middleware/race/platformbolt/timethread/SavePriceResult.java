package com.alibaba.middleware.race.platformbolt.timethread;

import com.alibaba.middleware.race.platformbolt.data.PlatformData;
import com.alibaba.middleware.race.platformbolt.data.TimePrice;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.alibaba.middleware.race.utils.RaceConfig;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

/**
 * 把实时的交易金额存在Tair中
 * Created by qinjiawei on 16-7-8.
 */
public class SavePriceResult extends TimerTask{

    @Override
    public void run() {

        Iterator<Map.Entry<Long, TimePrice>> Taobaoentries = PlatformData.TaobaoStorgeMap.entrySet().iterator();
        //Tair 存储
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup, RaceConfig.TairNamespace);

        //遍历Taobao
        while (Taobaoentries.hasNext())
        {
            Map.Entry<Long, TimePrice> entry = Taobaoentries.next();
            double tairprice =(Double) tairOperator.get(RaceConfig.prex_taobao + entry.getKey());
            if (Math.abs(entry.getValue().getPrice() - tairprice) < 0.005)
                continue;
            tairOperator.write(RaceConfig.prex_taobao + entry.getKey(), entry.getValue().getPrice());


        }

        //遍历Tmall
        Iterator<Map.Entry<Long, TimePrice>> Tmallentries = PlatformData.TmallStorgeMap.entrySet().iterator();

        while (Tmallentries.hasNext())
        {
            Map.Entry<Long, TimePrice> entry = Tmallentries.next();
            double tairprice =(Double) tairOperator.get(RaceConfig.prex_tmall + entry.getKey());
            if (Math.abs(entry.getValue().getPrice() - tairprice) < 0.005)
                continue;
            tairOperator.write(RaceConfig.prex_tmall + entry.getKey(), entry.getValue().getPrice());

        }

    }
}
