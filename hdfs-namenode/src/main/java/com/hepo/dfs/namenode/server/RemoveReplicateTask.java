package com.hepo.dfs.namenode.server;

/**
 * Description: 删除副本任务
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/9 08:51
 *
 * @author linhaibo
 */
public class RemoveReplicateTask {
    private String filename;
    private DataNodeInfo datanode;

    public RemoveReplicateTask(String filename, DataNodeInfo datanode) {
        this.filename = filename;
        this.datanode = datanode;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public DataNodeInfo getDatanode() {
        return datanode;
    }

    public void setDatanode(DataNodeInfo datanode) {
        this.datanode = datanode;
    }
}
