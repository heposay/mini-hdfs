package com.hepo.dfs.client.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hepo.dfs.namenode.rpc.model.HeartbeatResponse;

import java.io.File;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.NAMENODE_HEARTBEAT_INTERVAL_TIME;

/**
 * Description: 心跳管理的组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/5 13:47
 *
 * @author linhaibo
 */
public class HeartbeatManager {


    public static final Integer SUCCESS = 1;
    public static final Integer FAILURE = 2;
    public static final Integer COMMAND_REGISTER = 1;
    public static final Integer COMMAND_REPORT_COMPLETE_STORAGE_INFO = 2;
    public static final Integer COMMAND_REPLICATE = 3;
    public static final Integer COMMAND_REMOVE_REPLICA = 4;

    /**
     * namenode的客户端
     */
    private final NameNodeRpcClient nameNodeRpcClient;

    /**
     * 磁盘存储管理的组件
     */
    private final StorageManager storageManager;

    private final ReplicateManager replicateManager;

    public HeartbeatManager(NameNodeRpcClient nameNodeRpcClient, StorageManager storageManager, ReplicateManager replicateManager) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.storageManager = storageManager;
        this.replicateManager = replicateManager;
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
                    if (SUCCESS.equals(response.getStatus())) {
                        JSONArray commands = JSONArray.parseArray(response.getCommands());
                        if (commands.size() > 0) {
                            for (int i = 0; i < commands.size(); i++) {
                                JSONObject command = commands.getJSONObject(i);
                                Integer type = command.getInteger("type");
                                JSONObject task = command.getJSONObject("content");
                                if (COMMAND_REPLICATE.equals(type)) {
                                    replicateManager.addReplicateTask(task);
                                    System.out.println("接收副本复制任务，" + command);
                                } else if (COMMAND_REMOVE_REPLICA.equals(type)) {
                                    System.out.println("接收副本删除任务，" + command);
                                    // 删除副本
                                    String filename = task.getString("filename");
                                    String absoluteFilenamePath = FileUtils.getAbsoluteFilenamePath(filename);
                                    File file = new File(absoluteFilenamePath);
                                    if (file.exists()) {
                                        file.delete();
                                    }
                                }
                            }
                        }
                    } else if (FAILURE.equals(response.getStatus())) {
                        //心跳失败
                        JSONArray commands = JSONArray.parseArray(response.getCommands());
                        for (int i = 0; i < commands.size(); i++) {
                            JSONObject command = commands.getJSONObject(i);
                            Integer type = command.getInteger("type");
                            if (COMMAND_REGISTER.equals(type)) {
                                //重新注册，针对DataNode重启的情况
                                nameNodeRpcClient.register();
                            } else if (COMMAND_REPORT_COMPLETE_STORAGE_INFO.equals(type)) {
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
