package com.hepo.hdfs.namenode.server;

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

    public NameNodeRpcServer(FSNamesystem namesystem) {
        this.namesystem = namesystem;
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
     * 启动这个Server服务
     */
    public void start() {
        System.out.println("开始监听指定的rpc server端口，来接收请求");
    }
}
