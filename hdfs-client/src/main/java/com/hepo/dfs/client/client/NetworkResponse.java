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
    private String requestId;
    private String hostname;
    private String ip;
    private Long fileLength;
    private ByteBuffer lengthBuffer;
    private ByteBuffer buffer;
    private Boolean isError;
    private Boolean isFinished;

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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getFileLength() {
        return fileLength;
    }

    public void setFileLength(Long fileLength) {
        this.fileLength = fileLength;
    }

    public ByteBuffer getLengthBuffer() {
        return lengthBuffer;
    }

    public void setLengthBuffer(ByteBuffer lengthBuffer) {
        this.lengthBuffer = lengthBuffer;
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

    public Boolean isFinished() {
        return isFinished;
    }

    public void setFinished(Boolean finished) {
        isFinished = finished;
    }
}
