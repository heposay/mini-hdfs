package com.hepo.dfs.client.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hepo.dfs.namenode.rpc.model.HeartbeatResponse;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.DATANAME_HONENAME;
import static com.hepo.dfs.client.datanode.server.DataNodeConfig.NAMENODE_HEARTBEAT_INTERVAL_TIME;

/**
 * Description: 心跳管理的组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/5 13:47
 *
 * @author linhaibo
 */
public class HeartbeatManager {

    /**
     * namenode的客户端
     */
    private final NameNodeRpcClient nameNodeRpcClient;

    /**
     * 磁盘存储管理的组件
     */
    private final StorageManager storageManager;

    public HeartbeatManager(NameNodeRpcClient nameNodeRpcClient, StorageManager storageManager) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.storageManager = storageManager;
    }

    /**
     * 开启心跳线程
     */
    public void start() {
        new HeartbeatThread().start();
    }

    /**
     * 负责心跳的后台线程
     */
    @SuppressWarnings("InfiniteLoopStatement")
    class HeartbeatThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    // 通过RPC接口发送到NameNode他的注册接口上去
                    HeartbeatResponse response = nameNodeRpcClient.heartbeat();
                    if (response.getStatus() == 1) {
                        System.out.println(DATANAME_HONENAME + ":发送心跳成功....");
                    } else if (response.getStatus() == 2) {
                        JSONArray commands = JSONArray.parseArray(response.getCommands());
                        for (int i = 0; i < commands.size(); i++) {
                            JSONObject command = commands.getJSONObject(i);
                            Integer type = command.getInteger("type");
                            if (type.equals(1)) {
                                //重新注册，针对DataNode重启的情况
                                nameNodeRpcClient.register();
                            } else if (type.equals(2)) {
                                StorageInfo storageInfo = storageManager.getStorageInfo();
                                if (storageInfo != null) {
                                    //全量上报，针对NameNode重启的情况
                                    nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
                                }
                            }
                        }
                    }
                    Thread.sleep(NAMENODE_HEARTBEAT_INTERVAL_TIME);
                } catch (Exception e) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    e.printStackTrace();
                }
            }
        }
    }


}
