package com.hepo.dfs.client.client;

import java.nio.ByteBuffer;

/**
 * Description: 网络响应对象
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 20:26
 *
 * @author linhaibo
 */
public class NetworkResponse {
    public static final String RESPONSE_SUCCESS = "SUCCESS";

    private String requestId;
    private String hostname;
    private String ip;
    private ByteBuffer buffer;
    private Boolean isError;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Boolean isError() {
        return isError;
    }

    public void setError(Boolean error) {
        isError = error;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
