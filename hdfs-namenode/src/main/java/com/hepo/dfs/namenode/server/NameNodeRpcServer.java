package com.hepo.dfs.namenode.server;

/**
 * Description: NameNode 的rpc服务接口
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:50
 *
 * @author linhaibo
 */
public class NameNodeRpcServer {

    /**
     * 负责管理元数据的组件
     */
    private FSNamesystem namesystem;

    /**
     * 负责管理datanode的组件
     */
    private DataNodeManager dataNodeManager;

    public NameNodeRpcServer(FSNamesystem namesystem, DataNodeManager dataNodeManager) {
        this.namesystem = namesystem;
        this.dataNodeManager = dataNodeManager;
    }

    /**
     * 创建目录
     * @param path
     * @return
     * @throws Exception
     */
    public Boolean mkdir(String path) throws Exception {
        return namesystem.mkdir(path);
    }

    /**
     * 对dataNode进行注册
     * @return
     */
    public Boolean register (String ip, String hostname) {
        return dataNodeManager.register(ip, hostname);
    }

    /**
     * 心跳检测
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean heartbeat(String ip, String hostname) {
        return dataNodeManager.heartbeat(ip, hostname);
    }

    /**
     * 启动这个Server服务
     */
    public void start() {
        System.out.println("开始监听指定的rpc server端口，来接收请求");
    }
}
