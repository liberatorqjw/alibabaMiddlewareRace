package com.alibaba.middleware.race.platformbolt.data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class PlatformData {

    public static ConcurrentHashMap<Long, TimePrice> TaobaoStorgeMap = new ConcurrentHashMap<Long, TimePrice>();
    public static ConcurrentHashMap<Long, TimePrice> TmallStorgeMap = new ConcurrentHashMap<Long, TimePrice>();

    public static ConcurrentHashMap<Long, OrderMap> OrderDataMap = new ConcurrentHashMap<Long, OrderMap>();

    public static LinkedBlockingQueue<PayData> PayAllData = new LinkedBlockingQueue<PayData>();



}
