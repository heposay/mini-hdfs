package com.hepo.dfs.namenode.server;

/**
 * Description: 负责管理内存中的edit logs的核心组件
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:44
 *
 * @author linhaibo
 */
public class FSEditLog {


    /**
     * 当前递增到的txid序号
     */
    private Long txidSeq = 0L;

    /**
     * 当前线程是否将内存数据刷到磁盘中
     */
    private volatile Boolean isSyncRunning = false;

    /**
     * 当前是否有线程在等待下一批edit log刷到磁盘中
     */
    private volatile Boolean isWaitSync = false;


    private volatile Boolean isSchedulingSync = false;

    /**
     * 当前同步到磁盘数据中最大的txid
     */
    private volatile Long syncTxid = 0L;

    /**
     * 每个线程本地的txid副本
     */
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();


    private DoubleBuffer doubleBuffer = new DoubleBuffer();

    /**
     * 记录editLog
     *
     * @param content log内容
     */
    public void logEdit(String content) {
        //这里必须得直接加锁,要保证editlog能顺序写，这样有两个好处：1.能保证editlog的tx都是顺序执行了，后面通过editLog来恢复数据也有数据保证，2.提高日志的写效率
        synchronized (this) {
            //刚进来就直接检查一下是否有人正在调度一次刷盘的操作
            waitSchedulingSync();

            // 获取全局唯一递增的txid，代表了edits log的序号
            txidSeq++;
            long txid = txidSeq;

            //放到本地线程副本变量里面
            localTxid.set(txid);

            //构建一个editLog对象
            EditLog editLog = new EditLog(txid, content);

            // 将edits log写入内存缓冲中，不是直接刷入磁盘文件
            doubleBuffer.write(editLog);

            //每次写完一条editslog之后，就应该检查一下当前这个缓冲区是否满了
            if (!doubleBuffer.shouldSyncToDisk()) {
                //如果缓冲区还没有满，直接return
                return ;
            }
            // 如果代码进行到这里，就说明需要刷磁盘
            isSchedulingSync = true;
        }
        //刷磁盘
        logSync();
    }

    /**
     * 等待正在调度的刷磁盘的操作
     */
    private void waitSchedulingSync() {
        try {
            // 此时就释放锁，等待一秒再次尝试获取锁，去判断
            // isSchedulingSync是否为false，就可以脱离出while循环
            while (isSchedulingSync) {
                wait(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    /**
     * 将内存数据刷到磁盘中
     * 这里允许一个线程一次性将内存所有的数据刷到磁盘文件中
     * 相当于实现一个批量将内存缓冲数据刷磁盘的过程
     */
    private void logSync() {
        //再次尝试加锁
        synchronized (this) {
            //假如某个线程已经将txid= 1,2,3,4,5的edit log刷到磁盘中或者正在刷到磁盘中
            //此时的syncMaxTxid = 5 代表正在刷到磁盘的最大txid
            //假如这时候有一个线程过来，他的txid = 3，此时该线程可以直接返回了，因为已经有线程正在把对应的edit log刷到磁盘中了
            Long txid = localTxid.get();

            //当前有线程在刷内存缓冲到磁盘的动作
            if (isSyncRunning) {
                //如果当先txid小于要刷盘的txid，说明已经有人已经在数据刷到盘中，直接返回
                if (txid <= syncTxid) {
                    return;
                }

                while (isSyncRunning) {
                    try {
                        //如果别人刷盘任务还没完成，释放锁等待一秒钟
                        wait(1000);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            //note：下面逻辑就是真正处理刷磁盘的事情
            //1.首先把两个缓冲区交换一下
            doubleBuffer.setReadyToSync();
            //获取当前缓冲区中最大的txid
            syncTxid = txid;

            //设置当前正在同步到磁盘的标志位
            isSchedulingSync = false;
            //唤醒那些还卡在waitSchedulingSync里面while循环那儿的线程，嘿哥们，我已经交换好缓冲区了，你们可以继续写日志了
            notifyAll();
            //将isSyncRunning标志位改成true，表示说，我要开始处理刷磁盘了，别人不要来打扰我
            isSyncRunning = true;
        }
        //开始将内存缓冲区的数据刷到磁盘中,这一步耗时会比较慢这，基本上肯定是毫秒级了，多的话要几十毫秒
        doubleBuffer.flush();
        synchronized (this) {
            //同步完文件数据，将isSyncRunning标志位复原
            isSyncRunning = false;
            //同时唤醒其他正在等待的线程
            notifyAll();
        }

    }


}
