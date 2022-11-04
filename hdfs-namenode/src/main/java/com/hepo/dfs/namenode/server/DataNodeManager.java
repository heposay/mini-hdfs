package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hepo.dfs.namenode.server.NameNodeConfig.*;

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
    public Boolean register(String ip, String hostname, Integer uploadServerPort) {
        DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, hostname, uploadServerPort);
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
     * 分配副本对应的数据节点
     * @param fileSize 文件大小
     * @return 数据节点集合
     */
    public List<DataNodeInfo> getAllocateDataNodes(long fileSize) {
        synchronized (this) {
            // 取出来所有的datanode，并且按照已经存储的数据大小来排序
            List<DataNodeInfo> datanodeList = new ArrayList<>(dataNodeInfoMap.values());
            Collections.sort(datanodeList);

            // 选择存储数据最少的头两个datanode出来
            List<DataNodeInfo> selectedDatanodes =  new ArrayList<>();
            if (datanodeList.size() > DATANODE_DUPLICATE)  {
                for (int i = 0; i < DATANODE_DUPLICATE; i++) {
                    selectedDatanodes.add(datanodeList.get(i));
                    //记录该节点已经存储数据的大小
                    selectedDatanodes.get(i).addStoredDataSize(fileSize);
                }
            }else {
                selectedDatanodes.addAll(datanodeList);
            }
            return selectedDatanodes;
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
