package com.hepo.dfs.namenode.server;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Description: 负责管理内存中的edit logs的核心组件
 * Project:  mini-hdfs
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
     * 是否正在调度一次刷盘的操作
     */
    private volatile Boolean isSchedulingSync = false;

    /**
     * 当前同步到磁盘数据中最大的txid
     */
    private volatile Long syncTxid = 0L;

    /**
     * 每个线程本地的txid副本
     */
    private final ThreadLocal<Long> localTxid = new ThreadLocal<>();

    /**
     * 内存双缓冲
     */
    private final DoubleBuffer doubleBuffer = new DoubleBuffer();

    /**
     * 管理元数据组件
     */
    private final FSNamesystem namesystem;

    public FSEditLog(FSNamesystem namesystem) {
        this.namesystem = namesystem;

        EditLogCleaner cleaner = new EditLogCleaner();
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /**
     * 记录editLog
     *
     * @param content log内容
     */
    public void logEdit(String content) {
        //这里必须得直接加锁,要保证editlog能顺序写，这样有两个好处：
        // 1.能保证editlog的tx都是顺序执行了，后面通过editLog来恢复数据也能保证顺序性，
        // 2.提高日志的写效率
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
            try {
                doubleBuffer.write(editLog);
            } catch (IOException e) {
                e.printStackTrace();
            }

            //每次写完一条editslog之后，就应该检查一下当前这个缓冲区是否满了
            if (!doubleBuffer.shouldSyncToDisk()) {
                //如果缓冲区还没有满，直接return
                return;
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
            Long txid = null;
            try {
                txid = localTxid.get();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                localTxid.remove();
            }

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
                    } catch (Exception e) {
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
        try {
            doubleBuffer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        synchronized (this) {
            //同步完文件数据，将isSyncRunning标志位复原
            isSyncRunning = false;
            //同时唤醒其他正在等待的线程
            notifyAll();
        }

    }

    /**
     * 强制将缓冲区数据刷到磁盘
     */
    public void flush() {
        try {
            doubleBuffer.setReadyToSync();
            doubleBuffer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取已经刷入磁盘的txid范围
     */
    public List<String> getFlushedTxids() {
        synchronized (this) {
            return doubleBuffer.getFlushedTxids();
        }
    }

    /**
     * 获取缓冲区的数据
     */
    public String[] getBufferedEditsLog() {
        synchronized (this) {
            return doubleBuffer.getBufferedEditsLog();
        }
    }

    /**
     * 自动清理editLog时间间隔
     */
    private static final long EDIT_LOG_CLEAN_INTERVAL = 40 * 1000;


    /**
     * 后台自动清理editlog文件
     */
    class EditLogCleaner extends Thread {

        @Override
        public void run() {
            System.out.println("editlog日志文件后台清理线程启动......");
            while (true) {
                try {
                    Thread.sleep(EDIT_LOG_CLEAN_INTERVAL);
                    //获取已经刷到磁盘的txid集合
                    List<String> flushedTxids = getFlushedTxids();
                    if (flushedTxids != null && flushedTxids.size() > 0) {
                        for (String flushedTxid : flushedTxids) {
                            //获取已经checkpoint的txid
                            long checkpointTxid = namesystem.getCheckpointTxid();

                            //数据切割，进行判断
                            long startTxid = Long.parseLong(flushedTxid.split(StringPoolConstant.UNDERLINE)[0]);
                            long endTxid = Long.parseLong(flushedTxid.split(StringPoolConstant.UNDERLINE)[1]);
                            if (checkpointTxid > endTxid) {
                                //发现刷入磁盘的endTxid比checkpointTxid还要小，说明该数据已经被写到checkpoint文件当中，可以删除该editLog文件
                                String path = "/Users/linhaibo/Documents/tmp/editslog/edits-" + startTxid + StringPoolConstant.DASH + endTxid + ".log";
                                File file = new File(path);
                                if (file.exists()) {
                                    file.delete();
                                    System.out.println("发现editlog日志文件不需要，进行删除：" + file.getPath());
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
