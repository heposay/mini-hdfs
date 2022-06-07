package com.hepo.dfs.datanode.server;

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

    /**
     * 开启心跳线程
     */
    public void startHeartbeat() {
        new HeartbeatThread().start();
    }

    /**
     * 负责注册的后天线程
     */
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
                // 我们写代码的时候，主要是在本地来运行和测试，有一些ip和hostname，就直接在代码里写死了
                // 大家后面自己可以留空做一些完善，你可以加一些配置文件读取的代码
                String ip = "127.0.0.1";
                String hostname = "dfs-data-01";
                // 通过RPC接口发送到NameNode他的注册接口上去
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    /**
     * 负责心跳的后台线程
     */
    class HeartbeatThread extends Thread {
        @Override
        public void run() {
            try {
                while(true) {
                    System.out.println("发送RPC请求到NameNode进行心跳.......");
                    String ip = "127.0.0.1";
                    String hostname = "dfs-data-01";
                    // 通过RPC接口发送到NameNode他的注册接口上去

                    Thread.sleep(30 * 1000); // 每隔30秒发送一次心跳到NameNode上去
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
