package com.alibaba.middleware.race.rationbolt.timethread;

import com.alibaba.middleware.race.rationbolt.data.RationData;
import com.alibaba.middleware.race.rationbolt.data.RationPrice;
import com.alibaba.middleware.race.sort.SortMap;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.alibaba.middleware.race.utils.RaceConfig;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class SaveRationResult extends TimerTask {

    //private ConcurrentHashMap<Long, RationPrice> RationPriceMap;

    /*
    public SaveRationResult(ConcurrentHashMap<Long, RationPrice> rationPriceMap) {
        RationPriceMap = rationPriceMap;
    }
*/
    public SaveRationResult() {
    }

    @Override
    public void run() {

        //对map进行排序 取出最近的creatTime
        SortMap sortMap = new SortMap(RationData.RationPriceMap);
        //long curtime = sortMap.getTheFirstTime();
        //List<Long> createTimes = sortMap.getTheFirstTime();

        Iterator<Long> iterator = sortMap.getTheFirstTime().iterator();

        double PCTotoal = 0.0;
        double WirlessTotal = 0.0;


        while (iterator.hasNext())
        {
            long createTime = iterator.next();

            PCTotoal += RationData.RationPriceMap.get(createTime).getPcPrice();
            WirlessTotal += RationData.RationPriceMap.get(createTime).getWirlessPrice();

            double ratio = WirlessTotal / PCTotoal;

            BigDecimal bg = new BigDecimal(ratio);

            double curRatio = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue(); //保留两位小数

            //Tair 存储
            TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup, RaceConfig.TairNamespace);

            //double oldPrice =(Double) tairOperator.get(RaceConfig.prex_ratio + curtime);
            //if (new BigDecimal(curRatio).compareTo(new BigDecimal(oldPrice)) ==0)
            //    continue;
            //if ( Math.abs(curRatio - oldPrice) < 0.005)
            //    continue;
            tairOperator.write(RaceConfig.prex_ratio + createTime, curRatio);


        }

    }
}
