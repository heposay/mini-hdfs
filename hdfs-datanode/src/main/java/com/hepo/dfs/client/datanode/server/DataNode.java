package com.hepo.dfs.client.datanode.server;

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

        heartbeatManager = new HeartbeatManager(nameNodeRpcClient, storageManager);
        heartbeatManager.start();

        FileUploadServer fileUploadServer = new FileUploadServer(nameNodeRpcClient);
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
