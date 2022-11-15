package com.hepo.dfs.client.datanode.server;

import java.nio.ByteBuffer;

/**
 * Description: 网络响应
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 15:44
 *
 * @author linhaibo
 */
public class NetworkResponse {
    private String clientAddr;

    private ByteBuffer buffer;

    public String getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
