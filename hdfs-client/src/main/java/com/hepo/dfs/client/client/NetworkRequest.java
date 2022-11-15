package com.hepo.dfs.client.client;

import java.nio.ByteBuffer;

/**
 * Description: 网络请求对象
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 18:48
 *
 * @author linhaibo
 */
public class NetworkRequest {

    public static final Integer REQUEST_TYPE = 4;
    public static final Integer FILENAME_LENGTH = 4;
    public static final Integer FILE_LENGTH = 8;
    public static final Integer REQUEST_SEND_FILE = 1;
    public static final Integer REQUEST_READ_FILE = 2;

    private String id;
    private Integer requestType;
    private String hostname;
    private String ip;
    private Integer uploadServerPort;
    private ByteBuffer buffer;
    private Boolean isNeedResponse;
    private long sendTime;

    private ResponseCallback callback;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getRequestType() {
        return requestType;
    }

    public void setRequestType(Integer requestType) {
        this.requestType = requestType;
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

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Boolean isNeedResponse() {
        return isNeedResponse;
    }

    public void setNeedResponse(Boolean needResponse) {
        isNeedResponse = needResponse;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public ResponseCallback getCallback() {
        return callback;
    }

    public void setCallback(ResponseCallback callback) {
        this.callback = callback;
    }
}
