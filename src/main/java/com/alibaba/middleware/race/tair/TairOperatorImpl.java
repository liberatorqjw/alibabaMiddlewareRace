package com.alibaba.middleware.race.tair;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

    List<String> confServers = new ArrayList<String>();


    //client
    DefaultTairManager tairManager = new DefaultTairManager();
   private int namespace;



    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        this.namespace = namespace;
        confServers.add(masterConfigServer);
        confServers.add(slaveConfigServer);
        tairManager.setConfigServerList(confServers);
        //grop name
        tairManager.setGroupName(groupName);

        //init client
        tairManager.init();
    }

    /**
     * 数据写进tair
     * @param key
     * @param value
     * @return
     */
    public boolean write(Serializable key, Serializable value) {
        ResultCode resultCode = tairManager.put(namespace, key, value, 0, 0);

        if (!resultCode.isSuccess())
            return false;
        return true;
    }

    public Object get(Serializable key) {

        Result<DataEntry> result = tairManager.get(namespace, key);
        if (result.isSuccess()) {
            DataEntry entry = result.getValue();
            if(entry != null) {
                // 数据存在
                return entry.getValue();

            } else {
                // 数据不存在
                return 0.0;
            }
        } else {
            // 异常处理
            return 0.0;
        }

    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }


    /*
    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write("test" + minuteTime, money);
        System.out.println(millisTime + "\n" + minuteTime);
        System.out.println(tairOperator.get("test" + minuteTime).toString());
    }
*/
}
