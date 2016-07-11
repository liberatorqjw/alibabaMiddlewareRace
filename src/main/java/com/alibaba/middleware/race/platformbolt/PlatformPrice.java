package com.alibaba.middleware.race.platformbolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.platformbolt.data.OrderMap;
import com.alibaba.middleware.race.platformbolt.data.PayData;
import com.alibaba.middleware.race.platformbolt.data.PlatformData;
import com.alibaba.middleware.race.platformbolt.data.TimePrice;
import com.alibaba.middleware.race.platformbolt.timethread.PayFindOrder;
import com.alibaba.middleware.race.platformbolt.timethread.SavePriceResult;
import com.alibaba.middleware.race.spout.MetaTuple;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.alibaba.middleware.race.utils.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class PlatformPrice implements IRichBolt {

    private static final Logger log = LoggerFactory.getLogger(PlatformPrice.class);
    private OutputCollector collector;
    private Lock lockcount;
    private Lock lockTmall;
    private Lock lockorder;
    private Lock lockTaobao;
    protected   ConcurrentHashMap<Long, OrderMap> TaobaoDataMap ;
    protected   ConcurrentHashMap<Long, OrderMap> TmallDataMap ;

    protected  ConcurrentHashMap<Long, TimePrice> Taobaoresult;
    protected  ConcurrentHashMap<Long, TimePrice> Tmallresult;

    protected HashMap<Long, Double> lastTaobaoresult;
    protected HashMap<Long, Double> lastTmallresult;
    protected LinkedBlockingDeque<PayData> NotFindPay;



    //private ConcurrentHashMap<Long, OrderMap> OrderDataMap;

    //private LinkedBlockingQueue<PayData> PayAllData;

     private static final long serialVersionUID = 2495121976857546346L;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector        = collector;
        this.lockcount = new ReentrantLock();
        this.lockTmall = new ReentrantLock();
        this.lockorder = new ReentrantLock();
        this.lockTaobao = new ReentrantLock();

        this.TaobaoDataMap = new ConcurrentHashMap<Long, OrderMap>();
        this.TmallDataMap = new ConcurrentHashMap<Long, OrderMap>();
        this.Taobaoresult = new ConcurrentHashMap<Long, TimePrice>();
        this.Tmallresult = new ConcurrentHashMap<Long, TimePrice>();
        this.lastTaobaoresult = new HashMap<Long, Double>();
        this.lastTmallresult = new HashMap<Long, Double>();
        this.NotFindPay = new LinkedBlockingDeque<PayData>();




        //this.OrderDataMap     = new ConcurrentHashMap<Long, OrderMap>();
        //this.PayAllData       = new LinkedBlockingQueue<PayData>();
        //this.TaobaoStorgeMap  = new ConcurrentHashMap<Long, TimePrice>();
        //this.TmallStorgeMap   = new ConcurrentHashMap<Long, TimePrice>();

        //开启一个监控线程来查看消息是不是全部到达
        //ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        //PayFindOrder payFindOrder = new PayFindOrder(OrderDataMap, PayAllData);
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        //service.scheduleAtFixedRate(payFindOrder, 10, 1, TimeUnit.SECONDS);
        Timer timerfind = new Timer();
        timerfind.schedule(new TimerTask() {
            @Override
            public void run() {
                findPayOrder();
            }
        }, 5 * 1000, 1 * 1000);

        //开启一个监控线程来查看消息是不是全部到达
        //ScheduledExecutorService Saveservice = Executors.newSingleThreadScheduledExecutor();
        //SavePriceResult savePriceResult = new SavePriceResult();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        //Saveservice.scheduleAtFixedRate(savePriceResult, 45, 45, TimeUnit.SECONDS);

        Timer timersave = new Timer();
        timersave.schedule(new TimerTask() {
            @Override
            public void run() {
                saveResult();
            }
        }, 10 * 1000, 30 * 1000);


    }

    @Override
    public void execute(Tuple input) {
        String topic = input.getString(0);
        Object message = input.getValue(1);


        if (topic.equals(RaceConfig.MqPayTopic))
        {
            MetaTuple metaTuple = (MetaTuple) message;

            PaymentMessage paymentMessage = metaTuple.getPaymentMessage();
            //订单id
            long orderid = paymentMessage.getOrderId();
            //订单的金额
            double price = paymentMessage.getPayAmount();

            collector.emit(new Values(topic, paymentMessage));
            //订单的交易时间
            long createTime = (paymentMessage.getCreateTime()/1000/60) * 60;


            PayData payData = new PayData(createTime, paymentMessage.getPayAmount(), paymentMessage.getOrderId());

            //存在订单
            if (TaobaoDataMap.containsKey(paymentMessage.getOrderId()))
            {
                TaobaoDataMap.get(orderid).descPrice(price);
                if (TaobaoDataMap.get(orderid).isZero())
                {
                    TaobaoDataMap.remove(orderid);
                }

                //结果map中还没有这个时间的结果
                if (!Taobaoresult.containsKey(createTime))
                {
                    lockTaobao.lock();

                    boolean add = true;
                    if (Taobaoresult.containsKey(createTime))
                    {
                        Taobaoresult.get(createTime).incrPrice(price);
                        add = false;
                    }
                    try {
                        if (add)
                        {
                            TimePrice timePrice = new TimePrice(price);
                            Taobaoresult.put(createTime, timePrice);
                        }
                    }
                    finally {
                        lockTaobao.unlock();
                    }
                }
                //结果时间里已经有了当前时间
                else
                {
                    Taobaoresult.get(createTime).incrPrice(price);
                }
                collector.ack(input);

            }
            //Tmall
            else if(TmallDataMap.containsKey(paymentMessage.getOrderId()))
            {
                //处理结果, 把时间把临时结果的map取出
                TmallDataMap.get(orderid).descPrice(price);
                if (TmallDataMap.get(orderid).isZero())
                {
                    TmallDataMap.remove(orderid);
                }

                //结果map中不存在
                if(!Tmallresult.containsKey(createTime))
                {
                    lockTmall.lock();
                    boolean add = true;

                    if (Tmallresult.containsKey(createTime))
                    {
                        Tmallresult.get(createTime).incrPrice(price);
                        add = false;
                    }
                    try {
                        if (add)
                        {
                            TimePrice timePrice = new TimePrice(price);
                            Tmallresult.put(createTime, timePrice);
                        }
                    }
                    finally {
                        lockTmall.unlock();
                    }
                }
                //结果map中有当前的时间
                else {
                    Tmallresult.get(createTime).incrPrice(price);
                }
                collector.ack(input);

            }
            //pay 没有找到order 就存在轮询队列里面
            else {

                   //重发
                   collector.fail(input);
            }


        }
        else if (topic.equals(RaceConfig.MqTaobaoTradeTopic))
        {

            OrderMessage orderMessage = (OrderMessage) message;
            OrderMap orderMap = new OrderMap(orderMessage.getTotalPrice(), RaceConfig.MqTaobaoTradeTopic);
            if (!TaobaoDataMap.containsKey(orderMessage.getOrderId())) {
                TaobaoDataMap.put(orderMessage.getOrderId(), orderMap);
                log.info("****save taobaodatamap {}", String.valueOf(orderMessage.getOrderId()) +" " +String.valueOf( orderMap.getTotalprice()));

            }
            collector.ack(input);

        }
        else if (topic.equals(RaceConfig.MqTmallTradeTopic))
        {

            OrderMessage orderMessage = (OrderMessage) message;
            OrderMap orderMap = new OrderMap(orderMessage.getTotalPrice(), RaceConfig.MqTmallTradeTopic);
            if (!TmallDataMap.containsKey(orderMessage.getOrderId())) {
                TmallDataMap.put(orderMessage.getOrderId(), orderMap);
                log.info("****save tmalldatamap {}", String.valueOf(orderMessage.getOrderId()) +" " +String.valueOf(orderMap.getTotalprice()));

            }
            collector.ack(input);
        }
    }

    @Override
    public void cleanup() {
        findPayOrder();
        saveResult();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("paytopic", "paymessage"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * pay 去order中找到匹配的order
     */
    public void findPayOrder()
    {
        Iterator<PayData> iters = NotFindPay.iterator();

        while (iters.hasNext()) {

            PayData payData = iters.next();

            long orderid = payData.getOrderid();
            long createTime = payData.getCreateTime();
            double price = payData.getCurprice();


            //存在订单
            if (TaobaoDataMap.containsKey(orderid))
            {
                log.info("find the TaobaoData in the queue {}", String.valueOf(payData.getOrderid())+String.valueOf(payData.getCurprice()));

               NotFindPay.remove(payData);

                TaobaoDataMap.get(orderid).descPrice(price);
                if (TaobaoDataMap.get(orderid).isZero())
                {
                    TaobaoDataMap.remove(orderid);
                }

                //结果map中还没有这个时间的结果
                if (!Taobaoresult.containsKey(createTime))
                {
                    lockTaobao.lock();

                    boolean add = true;
                    if (Taobaoresult.containsKey(createTime))
                    {
                        Taobaoresult.get(createTime).incrPrice(price);
                        add = false;
                    }
                    try {
                        if (add)
                        {
                            TimePrice timePrice = new TimePrice(price);
                            Taobaoresult.put(createTime, timePrice);
                        }
                    }
                    finally {
                        lockTaobao.unlock();
                    }
                }
                //结果时间里已经有了当前时间
                else
                {
                    Taobaoresult.get(createTime).incrPrice(price);
                }


            }
            //Tmall
            else if(TmallDataMap.containsKey(orderid))
            {
                log.info("find the Tmalldata in the queue {}", String.valueOf(payData.getOrderid())+" " +String.valueOf(payData.getCurprice()));
                NotFindPay.remove(payData);

                //处理结果, 把时间把临时结果的map取出
                TmallDataMap.get(orderid).descPrice(price);
                if (TmallDataMap.get(orderid).isZero())
                {
                    TmallDataMap.remove(orderid);
                }

                //结果map中不存在
                if(!Tmallresult.containsKey(createTime))
                {
                    lockTmall.lock();
                    boolean add = true;

                    if (Tmallresult.containsKey(createTime))
                    {
                        Tmallresult.get(createTime).incrPrice(price);
                        add = false;
                    }
                    try {
                        if (add)
                        {
                            TimePrice timePrice = new TimePrice(price);
                            Tmallresult.put(createTime, timePrice);
                        }
                    }
                    finally {
                        lockTmall.unlock();
                    }
                }
                //结果map中有当前的时间
                else {
                    Tmallresult.get(createTime).incrPrice(price);
                }

            }

        }

    }

    /**
     * 存储最后的结果
     */
    public void saveResult()
    {

        Iterator<Map.Entry<Long, TimePrice>> Taobaoentries = Taobaoresult.entrySet().iterator();
        //Tair 存储
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup, RaceConfig.TairNamespace);

        //遍历Taobao
        while (Taobaoentries.hasNext())
        {
            Map.Entry<Long, TimePrice> entry = Taobaoentries.next();
            //double tairprice =(Double) tairOperator.get(RaceConfig.prex_taobao + entry.getKey());

            BigDecimal bg = new BigDecimal(entry.getValue().getPrice());

            double curRatio = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue(); //保留两位小数

            if (lastTaobaoresult.containsKey(entry.getKey()) && (lastTaobaoresult.get(entry.getKey()) - curRatio) < 0.005)
                continue;
//            if (Math.abs(entry.getValue().getPrice() - tairprice) < 0.005)
//                continue;

            else {
                tairOperator.write(RaceConfig.prex_taobao + entry.getKey(), curRatio);
                lastTaobaoresult.put(entry.getKey(), curRatio);
                log.info("####save the result into the tair {}", String.valueOf(RaceConfig.prex_taobao + entry.getKey()) + ":" + String.valueOf(curRatio));

            }

        }

        //遍历Tmall
        Iterator<Map.Entry<Long, TimePrice>> Tmallentries = Tmallresult.entrySet().iterator();

        while (Tmallentries.hasNext())
        {
            Map.Entry<Long, TimePrice> entry = Tmallentries.next();
//            double tairprice =(Double) tairOperator.get(RaceConfig.prex_tmall + entry.getKey());
//            if (Math.abs(entry.getValue().getPrice() - tairprice) < 0.005)
//                continue;
            BigDecimal bg = new BigDecimal(entry.getValue().getPrice());

            double curRatio = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue(); //保留两位小数

            if (lastTmallresult.containsKey(entry.getKey()) && (lastTmallresult.get(entry.getKey()) - curRatio) < 0.005)
                    continue;
            else {

                tairOperator.write(RaceConfig.prex_tmall + entry.getKey(), curRatio);
                 lastTmallresult.put(entry.getKey(), curRatio);
                log.info("####save the result into the tair {}", String.valueOf(RaceConfig.prex_tmall +entry.getKey()) +":" +String.valueOf(curRatio));
            }
        }

    }

    /*
               //pay 没找到order
            if (!PlatformData.OrderDataMap.containsKey(payData.getOrderid()))
            {
                //存在pay中
                PlatformData.PayAllData.add(payData);
            }
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
                        lockcount.lock();
                        if (PlatformData.TaobaoStorgeMap.containsKey(payData.getCreateTime()))
                        {
                            PlatformData.TaobaoStorgeMap.get(payData.getCreateTime()).incrPrice(payData.getCurprice());

                        }
                        else {
                            TimePrice timePrice = new TimePrice(payData.getCurprice());

                            PlatformData.TaobaoStorgeMap.put(payData.getCreateTime(), timePrice);
                        }
                        lockcount.unlock();
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
                        lockTmall.lock();
                        if (PlatformData.TmallStorgeMap.containsKey(payData.getCreateTime()))
                        {
                            PlatformData.TmallStorgeMap.get(payData.getCreateTime()).incrPrice(payData.getCurprice());
                        }
                        else {
                            TimePrice timePrice = new TimePrice(payData.getCurprice());

                            PlatformData.TmallStorgeMap.put(payData.getCreateTime(), timePrice);
                        }
                        lockTmall.unlock();
                    }
                }

                if (PlatformData.OrderDataMap.get(payData.getOrderid()).getTotalprice() < 0.005)
                    PlatformData.OrderDataMap.remove(payData.getOrderid());
            }
     */
}
