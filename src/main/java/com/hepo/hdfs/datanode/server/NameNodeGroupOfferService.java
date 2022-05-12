package com.hepo.hdfs.datanode.server;

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

    public NameNodeGroupOfferService() {
        this.activeServiceActor = new NameNodeServiceActor();
        this.standbyServiceActor = new NameNodeServiceActor();
    }


    /**
     * 启动OfferService组件
     */
    public void start() {
        //直接使用两个ServiceActor组件分别向主备两个NameNode节点进行注册
        register();
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
