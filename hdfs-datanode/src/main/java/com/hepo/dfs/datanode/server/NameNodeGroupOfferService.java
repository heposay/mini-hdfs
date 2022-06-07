package com.hepo.dfs.datanode.server;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

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
    private NameNodeServiceActor activeServiceActor;

    /**
     * 负责跟NameNode备节点通信的ServiceActor组件
     */
    private NameNodeServiceActor standbyServiceActor;

    private CopyOnWriteArrayList<NameNodeServiceActor> serviceActors;

    public NameNodeGroupOfferService() {
        this.activeServiceActor = new NameNodeServiceActor();
        this.standbyServiceActor = new NameNodeServiceActor();

        serviceActors = new CopyOnWriteArrayList<>();
        serviceActors.add(activeServiceActor);
        serviceActors.add(standbyServiceActor);
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
        this.activeServiceActor.startHeartbeat();
        this.standbyServiceActor.startHeartbeat();
    }

    /**
     * 关闭指定的一个ServiceActor
     * @param serviceActor
     */
    public void shutdown(NameNodeServiceActor serviceActor) {
        this.serviceActors.remove(serviceActor);
    }

    /**
     * 向主备两个NameNode节点进行注册
     */
    private void register() {
        try {
            //同步器
            CountDownLatch latch = new CountDownLatch(2);
            this.activeServiceActor.register(latch);
            this.standbyServiceActor.register(latch);
            latch.await();
            System.out.println("主备NameNode全部注册完毕......");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
