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
import com.alibaba.middleware.race.utils.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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

    //private ConcurrentHashMap<Long, OrderMap> OrderDataMap;

    //private LinkedBlockingQueue<PayData> PayAllData;

     private static final long serialVersionUID = 2495121976857546346L;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector        = collector;
        this.lockcount = new ReentrantLock();
        this.lockTmall = new ReentrantLock();

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
        timerfind.schedule(new PayFindOrder(), 10 *1000, 1*1000);

        //开启一个监控线程来查看消息是不是全部到达
        //ScheduledExecutorService Saveservice = Executors.newSingleThreadScheduledExecutor();
        //SavePriceResult savePriceResult = new SavePriceResult();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        //Saveservice.scheduleAtFixedRate(savePriceResult, 45, 45, TimeUnit.SECONDS);

        Timer timersave = new Timer();
        timersave.schedule(new SavePriceResult(), 45 *1000, 45 *1000);


    }

    @Override
    public void execute(Tuple input) {
        String topic = input.getString(0);
        Object message = input.getValue(1);


        if (topic.equals(RaceConfig.MqPayTopic))
        {
            PaymentMessage paymentMessage = (PaymentMessage) message;
            collector.emit(new Values(topic, paymentMessage));

            PayData payData = new PayData((paymentMessage.getCreateTime() /1000 /60) * 60, paymentMessage.getPayAmount(), paymentMessage.getOrderId());

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
            }

        }
        else if (topic.equals(RaceConfig.MqTaobaoTradeTopic))
        {
            OrderMessage orderMessage = (OrderMessage) message;
            OrderMap orderMap = new OrderMap(orderMessage.getTotalPrice(), RaceConfig.MqTaobaoTradeTopic);
            if (!PlatformData.OrderDataMap.containsKey(orderMessage.getOrderId()))
                PlatformData.OrderDataMap.put(orderMessage.getOrderId(), orderMap);


        }
        else if (topic.equals(RaceConfig.MqTmallTradeTopic))
        {
            OrderMessage orderMessage = (OrderMessage) message;
            OrderMap orderMap = new OrderMap(orderMessage.getTotalPrice(), RaceConfig.MqTmallTradeTopic);
            if (!PlatformData.OrderDataMap.containsKey(orderMessage.getOrderId()))
                PlatformData.OrderDataMap.put(orderMessage.getOrderId(), orderMap);

        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("paytopic", "paymessage"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
