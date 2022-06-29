package com.hepo.dfs.client.datanode.server;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Description: 负责跟一组NameNode进行通信的OfferServie组件
 * Project:  hdfs_study
 * CreateDate: Created in 2022-05-06 14:25
 *
 * @author linhaibo
 */
public class NameNodeGroupOfferService {
    /**
     * 负责跟NameNode主节点通信的ServiceActor组件
     */
    private NameNodeServiceActor serviceActor;

    /**
     * 这个datanode上保存的ServiceActor列表
     */
    private CopyOnWriteArrayList<NameNodeServiceActor> serviceActors;

    public NameNodeGroupOfferService() {
        this.serviceActor = new NameNodeServiceActor();

        serviceActors = new CopyOnWriteArrayList<>();
    }


    /**
     * 启动OfferService组件
     */
    public void start() {
        //直接使用两个ServiceActor组件分别向主备两个NameNode节点进行注册
        register();
        // 开始发送心跳
        startHeartbeat();
    }

    /**
     * 开启心跳线程
     */
    private void startHeartbeat() {
        this.serviceActor.startHeartbeat();
    }

    /**
     * 关闭指定的一个ServiceActor
     * @param serviceActor
     */
    public void shutdown(NameNodeServiceActor serviceActor) {
        this.serviceActors.remove(serviceActor);
    }

    /**
     * 向NameNode节点进行注册
     */
    private void register() {
        try {
            this.serviceActor.register();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
