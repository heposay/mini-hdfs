package com.hepo.dfs.namenode.server;

/**
 * Description: namenode 核心启动类
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:54
 *
 * @author linhaibo
 */
public class NameNode {

    /**
     * 负责管理元数据的组件
     */
    private FSNamesystem namesystem;

    /**
     * namenode对外暴露rpc接口的server，可以相应请求
     */
    private NameNodeRpcServer rpcServer;

    /**
     * 判断nameNode是否运行的标志
     */
    private volatile Boolean isRunning;

    /**
     * 负责管理datanode的组件
     */
    private DataNodeManager dataNodeManager;

    public NameNode() {
        this.isRunning = true;
    }

    /**
     * 初始化namenode各个组件
     */
    public void initialize() {
        this.namesystem = new FSNamesystem();
        this.dataNodeManager = new DataNodeManager();
        this.rpcServer = new NameNodeRpcServer(namesystem, dataNodeManager);
        this.rpcServer.start();
    }


    /**
     * NameNode启动服务的run方法
     */
    private void run() {
        while (isRunning) {
            try {
                System.out.println("NameNode组件已启动");
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
        NameNode nameNode = new NameNode();
        nameNode.initialize();
        nameNode.run();
    }
}
