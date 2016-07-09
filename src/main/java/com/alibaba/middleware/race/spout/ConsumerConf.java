package com.alibaba.middleware.race.spout;

import com.alibaba.middleware.race.utils.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by qinjiawei on 16-6-28.
 */
public class ConsumerConf implements Serializable {

    private static Logger log = LoggerFactory.getLogger(ConsumerConf.class);
    private static final long           serialVersionUID = 4641537253577312163L;
    public static Map<String, DefaultMQPushConsumer> consumers = new HashMap<String, DefaultMQPushConsumer>();

    public static synchronized DefaultMQPushConsumer mkInstance(MessageListenerConcurrently listener) throws MQClientException {



        //log.info("start to init consumer client, configuration: ", JSON.toJSONString(config));

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        // MetaPushConsumer consumer = new MetaPushConsumer(RaceConfig.MetaConsumerGroup);
        String nameServer = RaceConfig.MqNameServ;
        //consumer.setNamesrvAddr(nameServer);

        String tags = "*";
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, tags);
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, tags);
        consumer.subscribe(RaceConfig.MqPayTopic, tags);

        consumer.setConsumeMessageBatchMaxSize(64);
        consumer.setPullBatchSize(128);
        consumer.setConsumeThreadMin(15);
        consumer.registerMessageListener(listener);

        //consumer.setPullThresholdForQueue(config.getQueueSize());
        //consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
//        consumer.setPullBatchSize(config.getPullBatchSize());
//        consumer.setPullInterval(config.getPullInterval());
//        consumer.setConsumeThreadMin(config.getPullThreadNum());
//        consumer.setConsumeThreadMax(config.getPullThreadNum());

        consumer.start();

        //consumers.put(key, consumer);
        log.info("Successfully create {} consumer", "all");

        return consumer;

    }

}

