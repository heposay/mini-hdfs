package com.hepo.hdfs.namenode.server;

import java.util.LinkedList;

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

    /**
     * 当前同步到磁盘数据中最大的txid
     */
    private volatile Long syncMaxTxid = 0L;

    /**
     * 每个线程本地的txid副本
     */
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();


    private DoubleBuffer editBuffer = new DoubleBuffer();

    /**
     * 记录editLog
     *
     * @param content log内容
     */
    public void log(String content) {
        //这里必须得直接加锁,要保证editlog能顺序写，这样有两个好处：1.能保证editlog的tx都是顺序执行了，后面通过editLog来恢复数据也有数据保证，2.提高日志的写效率
        synchronized (this) {
            // 获取全局唯一递增的txid，代表了edits log的序号
            txidSeq++;
            long txid = txidSeq;
            localTxid.set(txid);

            //构建一个editLog对象
            EditLog editLog = new EditLog(txid, content);

            // 将edits log写入内存缓冲中，不是直接刷入磁盘文件
            editBuffer.write(editLog);
        }
        logSync();
    }

    /**
     * 将内存数据刷到磁盘中
     * 这里允许一个线程一次性将内存所有的数据刷到磁盘文件中
     * 相当于实现一个批量将内存缓冲数据刷磁盘的过程
     */
    private void logSync() {
        //再次尝试加锁
        synchronized (this) {
            //当前有线程在刷内存缓冲到磁盘的动作
            if (isSyncRunning) {
                //假如某个线程已经将txid= 1,2,3,4,5的edit log刷到磁盘中或者正在刷到磁盘中
                //此时的syncMaxTxid = 5 代表正在刷到磁盘的最大txid
                //假如这时候有一个线程过来，他的txid = 3，此时该线程可以直接返回了，因为已经有线程正在把对应的edit log刷到磁盘中了
                Long txid = localTxid.get();
                if (txid <= syncMaxTxid) {
                    return;
                }
                //假如此时有一个线程过来，他的txid=12,会判断isWaitSync是否为true，如果是，则表明说现在已经有人进来排队等待了，该线程可以直接返回做其他事情，而不是阻塞在这里等待。
                if (isWaitSync) {
                    return;
                }
                //如果上面isWaitSync为false，表明当前还没有线程进来，赶紧把这个isWaitSync标志位改成true，就是告诉别人说，我已经把这个坑位占了，其他人就不要来了
                isWaitSync = true;
                while (isSyncRunning) {

                    try {
                        wait(2000);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //任务处理完，把标志位改成false，把对应的坑位让出来
                isWaitSync = false;
            }

            //下面逻辑就是真正处理刷磁盘的事情
            //1.首先把两个缓冲区交换一下
            editBuffer.setReadyToSync();
            //获取当前缓冲区中最大的txid
            syncMaxTxid = editBuffer.getSyncMaxTxid();
            //将isSyncRunning标志位改成true，表示说，我要开始处理刷磁盘了，别人不要来打扰我
            isSyncRunning = true;
        }
        //开始将内存缓冲区的数据刷到磁盘中,这一步耗时会比较慢这，基本上肯定是毫秒级了，弄不好就要几十毫秒
        editBuffer.flush();
        synchronized (this) {
            //同步完文件数据，将isSyncRunning标志位复原
            isSyncRunning = false;
            //同时唤醒其他正在等待的线程
            notifyAll();
        }

    }

    /**
     * editLog内部类，代表一条日志
     */
    class EditLog {
        /**
         * 当前日志的txid
         */
        Long txid;
        /**
         * 日志内容
         */
        String content;

        public EditLog(Long txid, String content) {
            this.txid = txid;
            this.content = content;
        }
    }

    /**
     * 内存双缓冲
     */
    class DoubleBuffer {
        /**
         * 是专门用来承载线程写入edits log
         */
        LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();

        /**
         * 专门用来将数据同步到另外一块缓冲
         */
        LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

        /**
         * 将日志写到缓冲中
         *
         * @param editLog 日志
         */
        public void write(EditLog editLog) {
            currentBuffer.add(editLog);
        }


        /**
         * 获取syncBuffer缓冲区中最大的txid
         *
         * @return
         */
        private Long getSyncMaxTxid() {
            return this.syncBuffer.getLast().txid;
        }

        /**
         * 交换两个缓冲区，为了同步内存数据到磁盘做准备
         */
        public void setReadyToSync() {
            LinkedList<EditLog> temp = currentBuffer;
            currentBuffer = syncBuffer;
            syncBuffer = temp;
        }

        /**
         * 将缓冲区的数据写到磁盘文件中去
         */
        public void flush() {
            for (EditLog editLog : syncBuffer) {
                System.out.println("将edit log写入磁盘文件中：" + editLog);
                //一般来说，就是用文件输出流将数据写到磁盘文件中去
            }
            //写完之后，清空syncBuffer缓冲区
            syncBuffer.clear();
        }
    }
}
