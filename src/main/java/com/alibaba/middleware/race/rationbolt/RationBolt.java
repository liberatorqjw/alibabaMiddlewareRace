package com.alibaba.middleware.race.rationbolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rationbolt.data.RationData;
import com.alibaba.middleware.race.rationbolt.data.RationPrice;
import com.alibaba.middleware.race.rationbolt.timethread.SaveRationResult;
import com.alibaba.middleware.race.utils.RaceConfig;

import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by qinjiawei on 16-7-8.
 */
public class RationBolt implements IRichBolt {

    private OutputCollector collector;

    //private ConcurrentHashMap<Long, RationPrice> RationPriceMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        //this.RationPriceMap = new ConcurrentHashMap<Long, RationPrice>();

        //开启一个监控线程来查看消息是不是全部到达
        //ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();


        //SaveRationResult savePlatformPrice = new SaveRationResult(RationPriceMap);
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        //service.scheduleAtFixedRate(savePlatformPrice, 45, 60, TimeUnit.SECONDS);
        Timer timer = new Timer();
        timer.schedule(new SaveRationResult(), 45*1000, 60 *1000);


    }

    @Override
    public void execute(Tuple input) {

        String topic = input.getString(0);
        PaymentMessage message = (PaymentMessage)input.getValue(1);

        short pc =0;
        short wireless =1;


        //格式化时间
        long createTime = (message.getCreateTime() /1000 /60 ) * 60;

        //已存在的整分时间
        if (RationData.RationPriceMap.containsKey(createTime))
        {
            if (message.getPayPlatform() == pc)
                RationData.RationPriceMap.get(createTime).incrPc(message.getPayAmount());
            else
            {
                RationData.RationPriceMap.get(createTime).incrWireless(message.getPayAmount());
            }
        }
        //还未存在的整分时间
        else
        {
            if (message.getPayPlatform() == pc)
            {
                RationPrice rationPrice = new RationPrice(message.getPayAmount(), 0.0);
                RationData.RationPriceMap.put(createTime, rationPrice);
            }
            else
            {

                RationPrice rationPrice = new RationPrice(0.0,message.getPayAmount());
                RationData.RationPriceMap.put(createTime, rationPrice);
            }
        }


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
