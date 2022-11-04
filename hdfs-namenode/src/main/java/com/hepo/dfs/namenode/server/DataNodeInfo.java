package com.hepo.dfs.namenode.server;

/**
 * Description: datanode的信息
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-05-27 10:32
 *
 * @author linhaibo
 */
public class DataNodeInfo implements Comparable<DataNodeInfo> {
    /**
     * ip地址
     */
    private String ip;
    /**
     * 机器名字
     */
    private String hostname;

    /**
     * uploadServer端口号
     */
    private int uplaodServerPort;
    /**
     * 最近一次心跳的时间
     */
    private Long latestHeartbeatTime;

    /**
     * 已经存储数据的大小
     */
    private long storedDataSize;

    public Long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public void setLatestHeartbeatTime(Long latestHeartbeatTime) {
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public DataNodeInfo(String ip, String hostname, Integer uplaodServerPort) {
        this.ip = ip;
        this.hostname = hostname;
        this.uplaodServerPort = uplaodServerPort;
        this.latestHeartbeatTime = System.currentTimeMillis();
        this.storedDataSize = 0L;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getStoredDataSize() {
        return storedDataSize;
    }

    public void setStoredDataSize(long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    public void addStoredDataSize(long storedDataSize) {
        this.storedDataSize += storedDataSize;
    }

    public int getUplaodServerPort() {
        return uplaodServerPort;
    }

    public void setUplaodServerPort(int uplaodServerPort) {
        this.uplaodServerPort = uplaodServerPort;
    }

    @Override
    public int compareTo(DataNodeInfo o) {
        if (this.storedDataSize > o.getStoredDataSize()) {
            return 1;
        }else if (this.storedDataSize < o.getStoredDataSize()) {
            return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "ip='" + ip + '\'' +
                ", hostname='" + hostname + '\'' +
                ", latestHeartbeatTime=" + latestHeartbeatTime +
                ", storedDataSize=" + storedDataSize +
                '}';
    }
}
