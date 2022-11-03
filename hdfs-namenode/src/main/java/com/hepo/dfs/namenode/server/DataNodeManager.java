package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: 这个组件，就是负责管理集群里的所有的datanode的
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-05-27 10:33
 *
 * @author linhaibo
 */
public class DataNodeManager {

    /**
     * 内存中维护的datanode列表
     */
    private Map<String, DataNodeInfo> dataNodeInfoMap = new ConcurrentHashMap<>();

    /**
     * 心跳过期时间
     */
    private final static long HEARTBEAT_LAST_EXPIRATION_TIME = 90 * 1000;

    /**
     * 心跳检测时间间隙
     */
    private final static long HEARTBEAT_CHECK_INTERVAL_TIME = 30 * 1000;


    public DataNodeManager() {
        DataNodeAliveMonitor dataNodeAliveMonitor = new DataNodeAliveMonitor();
        dataNodeAliveMonitor.setDaemon(true);
        dataNodeAliveMonitor.setName("DataNodeAliveMonitor-");
        dataNodeAliveMonitor.start();
    }

    public Map<String, DataNodeInfo> getDataNodeInfoMap() {
        return dataNodeInfoMap;
    }

    public void setDataNodeInfoMap(Map<String, DataNodeInfo> dataNodeInfoMap) {
        this.dataNodeInfoMap = dataNodeInfoMap;
    }


    /**
     * 对datanode进行注册
     */
    public Boolean register(String ip, String hostname) {
        DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, hostname);
        dataNodeInfoMap.put(ip + "-" + hostname, dataNodeInfo);
        return true;
    }

    /**
     * datanode进行心跳
     */
    public Boolean heartbeat(String ip, String hostname) {
        DataNodeInfo dataNodeInfo = dataNodeInfoMap.get(ip + "-" + hostname);
        if (dataNodeInfo != null) {
            dataNodeInfo.setLatestHeartbeatTime(System.currentTimeMillis());
            return true;
        }
        return false;
    }

    /**
     * 将datanodeInfoMap元数据刷到磁盘中
     */
    public void flush() {
        FileOutputStream fos = null;
        FileChannel channel = null;
        try {
            String path = "/Users/linhaibo/Documents/tmp/datanode/datanode-info.meta";
            fos = new FileOutputStream(path);
            channel = fos.getChannel();

            ByteBuffer buffer = ByteBuffer.wrap(JSONObject.toJSONString(dataNodeInfoMap).getBytes());
            channel.write(buffer);
            channel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }


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
                        //遍历所有的datanode节点的心跳时间，如果心跳时间超过90秒没有更新，说明该节点已经离线，则把该服务摘除
                        dataNodeInfo = iterator.next();
                        if (System.currentTimeMillis() - dataNodeInfo.getLatestHeartbeatTime() > HEARTBEAT_LAST_EXPIRATION_TIME) {
                            toRemoveDatanodes.add(dataNodeInfo.getIp() + "-" + dataNodeInfo.getHostname());
                        }
                    }
                    if (!toRemoveDatanodes.isEmpty()) {
                        for (String toRemoveDatanode : toRemoveDatanodes) {
                            dataNodeInfoMap.remove(toRemoveDatanode);
                        }
                    }
                    Thread.sleep(HEARTBEAT_CHECK_INTERVAL_TIME);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
