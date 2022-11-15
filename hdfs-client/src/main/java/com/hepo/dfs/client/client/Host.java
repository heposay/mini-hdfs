package com.hepo.dfs.client.client;

/**
 * Description: 代表一台DataNode的机器
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 23:58
 *
 * @author linhaibo
 */
public class Host {
    private String hostname;
    private String ip;
    private Integer uploadServerPort;

    public Host() {
    }

    public Host(String hostname, Integer uploadServerPort) {
        this.hostname = hostname;
        this.uploadServerPort = uploadServerPort;
    }

    public Host(String hostname, String ip, Integer uploadServerPort) {
        this.hostname = hostname;
        this.ip = ip;
        this.uploadServerPort = uploadServerPort;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getUploadServerPort() {
        return uploadServerPort;
    }

    public void setUploadServerPort(Integer uploadServerPort) {
        this.uploadServerPort = uploadServerPort;
    }

    public String getId() {
        return hostname + "-" + ip;
    }
}
