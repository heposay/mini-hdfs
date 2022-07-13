package com.hepo.dfs.namenode.server;

/**
 * Description: 负责管理组件的所有元数据
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:46
 *
 * @author linhaibo
 */
public class FSNamesystem {

    /**
     * 负责管理内存中文件目录树的组件
     */
    private FSDirectory directory;

    /**
     * 负责管理内存中edit log的组件
     */
    private FSEditLog editLog;

    /**
     * 最近一次checkpoint更新的txid
     */
    private long checkpointTxid;

    /**
     * 初始化组件
     */
    public FSNamesystem() {
        directory = new FSDirectory();
        editLog = new FSEditLog(this);
    }

    /**
     * 创建目录
     *
     * @param path 文件路径
     * @return 是否成功
     */
    public Boolean mkdir(String path) {
        this.directory.mkdir(path);
        this.editLog.logEdit("{'OP':'MKDIR', 'PATH':'" + path + "'}");
        return true;
    }

    /**
     * 强制将缓冲区的数据刷到磁盘
     */
    public void flush() {
        this.editLog.flush();
    }

    /**
     * 获取FSEditLog组件
     * @return
     */
    public FSEditLog getEditLog() {
        return editLog;
    }


    public long getCheckpointTxid() {
        return checkpointTxid;
    }

    public void setCheckpointTxid(long checkpointTxid) {
        System.out.println("接收到checkpoint txid" + checkpointTxid);
        this.checkpointTxid = checkpointTxid;
    }
}
