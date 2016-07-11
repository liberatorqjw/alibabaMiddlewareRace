
package com.alibaba.middleware.race.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.utils.JStormUtils;
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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class defaultConsumerSpout implements IRichSpout, MessageListenerConcurrently, IAckValueSpout, IFailValueSpout {

    protected Map conf;
    protected String id;
    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer consumer;
    
	private static final long serialVersionUID = 8476906628618859716L;
    private static Logger log = LoggerFactory.getLogger(defaultConsumerSpout.class);
    protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

    protected boolean flowControl;
    protected boolean autoAck;

    protected AtomicInteger acknums;

    protected AtomicInteger failnums;

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
        this.sendingQueue = new LinkedBlockingDeque<MetaTuple>();
        this.flowControl = true;

        this.autoAck = false;

        acknums = new AtomicInteger(0);
        failnums = new AtomicInteger(0);

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
        MetaTuple metaTuple = null;
        try {
            metaTuple = sendingQueue.take();
        } catch (InterruptedException e) {
        }

        if (metaTuple == null) {
            return;
        }

        sendTuple(metaTuple);

    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    //// FIXME: 16/6/3 harlenzhang need to consider the cosuming of message
    /*
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
    */

    public void sendTuple(MetaTuple metaTuple) {
        //metaTuple.updateEmitMs();
        collector.emit(new Values(RaceConfig.MqPayTopic, metaTuple), metaTuple);
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

        for (MessageExt msg : msgs) {

            String topic = msg.getTopic();

            byte[] bodyByte = msg.getBody();
            if (bodyByte.length == 2 && bodyByte[0] == 0 && bodyByte[1] == 0) {
                //logger.info(RaceConfig.LogTracker + "ZY spout" + topic + "ends soon");
                continue;
            }

            if (topic.equals(RaceConfig.MqPayTopic))
            {
                //存队列
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, bodyByte);
                //log.info("send pay message");
                // 在flowControl模式下 使用阻塞队列来发送Pay的信息
                if (flowControl) {
                    try {
                        sendingQueue.put(new MetaTuple(paymentMessage, 0));
                    } catch (InterruptedException e) {
                        //logger.info(RaceConfig.LogTracker + "ZY spout put PAY operation interupt:" + e.getMessage(), e);
                    }
                } else {
                    sendTuple(new MetaTuple(paymentMessage,0));
                }

            }
            else
            {
                //log.info("send order message");
                OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, bodyByte);
                collector.emit(new Values(topic, orderMessage), orderMessage);
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

    }

        @Override
    public void ack(Object msgId, List<Object> values) {


            if (flowControl) {
                int ackNum = acknums.incrementAndGet();
                int failNum = failnums.get();
                if (ackNum >= 100000 && failNum != 0 && ((ackNum / failNum) >= 20)) {
                   flowControl = false;
                    acknums.set(0);
                    failnums.set(0);
                }
            }


        }

    @Override
    public void fail(Object msgId, List<Object> values) {

        if (!flowControl){
            int nums = failnums.incrementAndGet();
            int ack = acknums.get();
            if (nums >=100000 && nums !=0 && (ack/nums) <=5)
            {
                flowControl = true;
                acknums.set(0);
                failnums.set(0);
            }
        }

        if (msgId instanceof MetaTuple)
        {
            MetaTuple message = (MetaTuple) values.get(1);
            message.incrfail();
            if (message.getFailnums() <= 5)
            {
                sendingQueue.offer(message);
            }
        }

    }
}
