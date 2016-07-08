
package com.alibaba.middleware.race.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.spout.ConsumerConf;
import com.alibaba.middleware.race.utils.RaceConfig;
import com.alibaba.middleware.race.utils.RaceUtils;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class defaultConsumerSpout implements IRichSpout, MessageListenerConcurrently{

    private Map conf;
    private String id;
    private SpoutOutputCollector collector;
    private transient DefaultMQPushConsumer consumer;

    private static Logger log = LoggerFactory.getLogger(defaultConsumerSpout.class);


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topic", "message"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.collector = collector;
        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();


        try {
            consumer = ConsumerConf.mkInstance(this);
        } catch (MQClientException e) {
            //log.error("failed to create rocket consumer: {}", e.getErrorMessage());
            throw new RuntimeException("fail to create consumer for component: " + id);
        }

        /**when there was consumer already been started, the consumer will be null*/
        if (consumer == null){
            //log.warn("component {} already have consumer fetch data", id);

            new Thread(new Runnable() {

                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            break;
                        }
                        //log.info("there was one consumer already started, thus the second will do nothing");

                    }
                }
            }).start();
        }
        //log.info("Successfully init " + id);

    }

    public void close() {
        if (consumer != null)
            consumer.shutdown();

    }

    public void activate() {
        if (consumer != null)
            consumer.resume();
    }

    public void deactivate() {
        if (consumer != null)
            consumer.suspend();

    }

    public void nextTuple() {
        //do nothing since the tuple was emitted in consumeMessage
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    //// FIXME: 16/6/3 harlenzhang need to consider the cosuming of message
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs){

            String topic = msg.getTopic();
            byte [] bodyByte = msg.getBody();
            if (topic.equals(RaceConfig.MqPayTopic))
            {
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, bodyByte);
                collector.emit(new Values(topic, paymentMessage));

            }
            else
            {
                OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, bodyByte);
                collector.emit(new Values(topic, orderMessage));
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
