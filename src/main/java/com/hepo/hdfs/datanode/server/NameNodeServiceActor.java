package com.hepo.hdfs.datanode.server;

import java.util.concurrent.CountDownLatch;

/**
 * Description: 负责跟一组NameNode中的某一个进行通信的线程组件
 * Project:  hdfs_study
 * CreateDate: Created in 2022-05-06 14:25
 *
 * @author linhaibo
 */
public class NameNodeServiceActor {

    /**
     * 向自己负责通信的那个NameNode进行注册
     */
    public void register(CountDownLatch latch) {
        RegisterThread registerThread = new RegisterThread(latch);
        registerThread.start();
    }

    class RegisterThread extends Thread {

        CountDownLatch latch;

        RegisterThread(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                // 发送rpc接口调用请求到NameNode去进行注册
                System.out.println("发送请求到NameNode进行注册.......");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}
