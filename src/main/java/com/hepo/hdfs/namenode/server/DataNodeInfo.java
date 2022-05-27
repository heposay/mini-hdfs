package com.hepo.hdfs.namenode.server;

/**
 * Description: datanode的信息
 * Project:  hdfs-study
 * CreateDate: Created in 2022-05-27 10:32
 *
 * @author linhaibo
 */
public class DataNodeInfo {

    private String ip;

    private String hostname;

    private Long latestHeartbeatTime;

    public Long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public void setLatestHeartbeatTime(Long latestHeartbeatTime) {
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public DataNodeInfo(String ip, String hostname) {
        this.ip = ip;
        this.hostname = hostname;
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
}
