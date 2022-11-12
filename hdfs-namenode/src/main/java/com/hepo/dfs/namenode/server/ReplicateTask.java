package com.hepo.dfs.namenode.server;

/**
 * Description: 副本复制任务
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/9 08:49
 *
 * @author linhaibo
 */
public class ReplicateTask {
    private String filename;
    private Long fileLength;
    private DataNodeInfo sourceDataNode;
    private DataNodeInfo destDatanode;


    public ReplicateTask(String filename, Long fileLength, DataNodeInfo sourceDataNode, DataNodeInfo destDatanode) {
        this.filename = filename;
        this.fileLength = fileLength;
        this.sourceDataNode = sourceDataNode;
        this.destDatanode = destDatanode;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Long getFileLength() {
        return fileLength;
    }

    public void setFileLength(Long fileLength) {
        this.fileLength = fileLength;
    }

    public DataNodeInfo getSourceDataNode() {
        return sourceDataNode;
    }

    public void setSourceDataNode(DataNodeInfo sourceDataNode) {
        this.sourceDataNode = sourceDataNode;
    }

    public DataNodeInfo getDestDatanode() {
        return destDatanode;
    }

    public void setDestDatanode(DataNodeInfo destDatanode) {
        this.destDatanode = destDatanode;
    }

    @Override
    public String toString() {
        return "ReplicateTask{" +
                "filename='" + filename + '\'' +
                ", fileLength=" + fileLength +
                ", sourceDataNode=" + sourceDataNode +
                ", destDatanode=" + destDatanode +
                '}';
    }

}

