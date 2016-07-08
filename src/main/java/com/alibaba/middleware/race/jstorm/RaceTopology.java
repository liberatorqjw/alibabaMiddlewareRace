package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.platformbolt.PlatformPrice;
import com.alibaba.middleware.race.rationbolt.RationBolt;
import com.alibaba.middleware.race.spout.defaultConsumerSpout;
import com.alibaba.middleware.race.utils.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        int spout_Parallelism_hint = 4;
        int split_Parallelism_hint = 1;
        int count_Parallelism_hint = 2;
        conf.setNumWorkers(3);
        //conf.setNumAckers(1);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("ConsumerSpout", new defaultConsumerSpout(), spout_Parallelism_hint);
        //builder.setBolt("MapBolt", new MapBolt()).shuffleGrouping("ConsumerSpout");
        //builder.setBolt("PriceCountBolt", new PriceCounterBolt()).shuffleGrouping("MapBolt");
        //builder.setBolt("CountPriceBolt", new CountPriceBolt()).shuffleGrouping("ConsumerSpout");
        builder.setBolt("DistributeBolt", new PlatformPrice()).shuffleGrouping("ConsumerSpout");
        builder.setBolt("SearchOrderAddBolt", new RationBolt()).shuffleGrouping("DistributeBolt");


        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            //LocalCluster cluster = new LocalCluster();
            //cluster.submitTopology(topologyName +"_local", conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
