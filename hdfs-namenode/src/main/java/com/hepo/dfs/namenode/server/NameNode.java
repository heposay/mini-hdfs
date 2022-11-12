package com.hepo.dfs.namenode.server;

import java.io.IOException;

/**
 * Description: namenode 核心启动类
 * Project:  mini-hdfs
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
     * 负责管理datanode的组件
     */
    private DataNodeManager dataNodeManager;

    private FSImageUploadServer fsImageUploadServer;


    /**
     * 初始化namenode各个组件
     */
    public void initialize() {
        this.dataNodeManager = new DataNodeManager();
        this.namesystem = new FSNamesystem(dataNodeManager);
        this.dataNodeManager.setNamesystem(namesystem);
        this.rpcServer = new NameNodeRpcServer(namesystem, dataNodeManager);
        fsImageUploadServer = new FSImageUploadServer();
    }


    public void start() throws IOException, InterruptedException {
        fsImageUploadServer.start();
        rpcServer.start();
        rpcServer.blockUntilShutdown();
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        NameNode nameNode = new NameNode();
        nameNode.initialize();
        nameNode.start();
    }
}
