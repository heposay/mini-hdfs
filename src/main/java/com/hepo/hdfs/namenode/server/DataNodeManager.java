package com.hepo.hdfs.namenode.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: 这个组件，就是负责管理集群里的所有的datanode的
 * Project:  hdfs-study
 * CreateDate: Created in 2022-05-27 10:33
 *
 * @author linhaibo
 */
public class DataNodeManager {

    /**
     * 内存中维护的datanode列表
     */
    private Map<String, DataNodeInfo> dataNodeInfoMap = new ConcurrentHashMap<>();


    public DataNodeManager() {
        DataNodeAliveMonitor dataNodeAliveMonitor = new DataNodeAliveMonitor();
        dataNodeAliveMonitor.setDaemon(true);
        dataNodeAliveMonitor.setName("DataNodeAliveMonitor-");
        dataNodeAliveMonitor.start();
    }
    /**
     * 对datanode进行注册
     *
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean register(String ip, String hostname) {
        DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, hostname);
        dataNodeInfoMap.put(ip + "-" + hostname, dataNodeInfo);
        return true;
    }

    /**
     * datanode进行心跳
     *
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean heartbeat(String ip, String hostname) {
        DataNodeInfo dataNodeInfo = dataNodeInfoMap.get(ip + "-" + hostname);
        if (dataNodeInfo != null) {
            dataNodeInfo.setLatestHeartbeatTime(System.currentTimeMillis());
        }
        return true;
    }

    /**
     * datanode是否存活的监控线程
     */
    class DataNodeAliveMonitor extends Thread {

        @Override
        public void run() {
            try {
                while (true) {
                    List<String> toRemoveDatanodes = new ArrayList<>();
                    Iterator<DataNodeInfo> iterator = dataNodeInfoMap.values().iterator();
                    DataNodeInfo dataNodeInfo = null;
                    while (iterator.hasNext()) {
                        dataNodeInfo = iterator.next();
                        if (System.currentTimeMillis() - dataNodeInfo.getLatestHeartbeatTime() > 90 * 1000) {
                            toRemoveDatanodes.add(dataNodeInfo.getIp() + "-" + dataNodeInfo.getHostname());
                        }
                    }
                    if (!toRemoveDatanodes.isEmpty()) {
                        for (String toRemoveDatanode : toRemoveDatanodes) {
                            dataNodeInfoMap.remove(toRemoveDatanode);
                        }
                    }
                    Thread.sleep(30 * 1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
