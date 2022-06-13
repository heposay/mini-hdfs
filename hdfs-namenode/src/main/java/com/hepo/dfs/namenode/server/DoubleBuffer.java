package com.hepo.dfs.namenode.server;

/**
 * 内存双缓冲
 * @author linhaibo
 */
public class DoubleBuffer {
    /**
     * 是专门用来承载线程写入edits log
     */
    EditBuffer currentBuffer = new EditBuffer();

    /**
     * 专门用来将数据同步到另外一块缓冲
     */
    EditBuffer syncBuffer = new EditBuffer();

    /**
     * 设置缓冲区大小，0.5M
     */
    private static final Long EDIT_LOG_BUFFER_LIMIT =512 * 1024L;


    /**
     * 将editLog写到缓冲区
     * @param log
     */
    public void write(EditLog log) {
        currentBuffer.write(log);
    }

    /**
     * 判断一下当前的缓冲区是否写满了需要刷到磁盘上去
     * @return
     */
    public boolean shouldSyncToDisk() {
        if (currentBuffer.size() >= EDIT_LOG_BUFFER_LIMIT ) {
            return true;
        }
        return false;
    }

    /**
     * 交换两个缓冲区，为了同步内存数据到磁盘做准备
     */
    public void setReadyToSync() {
        EditBuffer temp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = temp;
    }

    /**
     * 将缓冲区刷到磁盘上
     */
    public void flush() {
        syncBuffer.flush();
        syncBuffer.clear();
    }

    /**
     * editlog缓冲区
     */
    class EditBuffer {

        /**
         * 将日志写到缓冲中
         *
         * @param editLog 日志
         */
        public void write(EditLog editLog) {

        }

        /**
         * 返回当前缓冲区大小
         * @return
         */
        public long size() {
            return 0L;
        }

        /**
         * 清空缓冲区
         */
        public void clear() {

        }

        /**
         * 将缓冲区的数据写到磁盘文件中去
         */
        public void flush() {

        }
    }
}