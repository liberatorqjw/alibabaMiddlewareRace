package com.alibaba.middleware.race.rationbolt.data;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class RationData {

    public static ConcurrentHashMap<Long, RationPrice> RationPriceMap = new ConcurrentHashMap<Long, RationPrice>();


}
