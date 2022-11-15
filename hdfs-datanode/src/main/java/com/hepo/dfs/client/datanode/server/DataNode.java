package com.hepo.dfs.client.datanode.server;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.DATA_DIR;

/**
 * Description: DataNode启动类
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-05-06 14:24
 *
 * @author linhaibo
 */
public class DataNode {

    /**
     * 运行标志
     */
    private volatile Boolean isRunning;

    /**
     * 负责跟一组NameNode通信的组件
     */
    private final NameNodeRpcClient nameNodeRpcClient;

    /**
     * 磁盘存储管理的组件
     */
    private final StorageManager storageManager;

    /**
     * DataNode心跳管理器
     */
    private final HeartbeatManager heartbeatManager;

    /**
     * 复制任务管理组件
     */
    private final ReplicateManager replicateManager;

    /**
     * DataNode初始化
     */
    public DataNode() {
        this.isRunning = true;
        this.nameNodeRpcClient = new NameNodeRpcClient();
        Boolean registerResult = nameNodeRpcClient.register();

        this.storageManager = new StorageManager();
        if (registerResult) {
            StorageInfo storageInfo = storageManager.getStorageInfo();
            if (storageInfo != null) {
                this.nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
            }
        }else {
            System.out.println("不需要全量上报存储信息......");
        }

        replicateManager = new ReplicateManager(nameNodeRpcClient);

        System.out.println("DataDir:" + DATA_DIR);

        heartbeatManager = new HeartbeatManager(nameNodeRpcClient, storageManager, replicateManager);
        heartbeatManager.start();

        FileUploadServer fileUploadServer = new FileUploadServer(nameNodeRpcClient);
        fileUploadServer.init();
        fileUploadServer.start();
    }


    private void run() {
        while (isRunning) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        this.isRunning = false;
    }

    public static void main(String[] args) {
        DataNode dataNode = new DataNode();
        dataNode.run();
    }

}
