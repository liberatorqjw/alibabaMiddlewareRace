package com.alibaba.middleware.race.sort;

import com.alibaba.middleware.race.rationbolt.data.RationData;
import com.alibaba.middleware.race.rationbolt.data.RationPrice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qinjiawei on 16-7-3.
 */
public class SortMap {



    private ConcurrentHashMap<Long, RationPrice> RationPriceMap;

    public SortMap(ConcurrentHashMap<Long, RationPrice> rationPriceMap) {
        RationPriceMap = rationPriceMap;
    }

    public SortMap() {
    }

    /**
     * 按照map的key进行排序,并且取出在首位的key
     */
    public List<Long> getTheFirstTime()
    {

        Set<Long> timesSet = RationData.RationPriceMap.keySet();
        List<Long> times = new ArrayList<Long>(timesSet);

        if (times.size() < 1)
            return new ArrayList<Long>();

        SetComparator setComparator =new SetComparator();
        Collections.sort(times, setComparator);
        return times;

    }
}
