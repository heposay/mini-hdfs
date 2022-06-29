package com.hepo.dfs.namenode.rpc.server.test;

import com.hepo.dfs.namenode.rpc.model.HeartbeatRequest;
import com.hepo.dfs.namenode.rpc.model.HeartbeatResponse;
import com.hepo.dfs.namenode.rpc.model.RegisterRequest;
import com.hepo.dfs.namenode.rpc.model.RegisterResponse;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

/**
 * Description:
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-29 09:02
 *
 * @author linhaibo
 */
public class ClientTest {

    //创建客户端的stub存根
    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
    private String DEFAULT_HOSTNAME = "localhost";
    private int DEFAULT_PORT = 50070;

    /**
     * 初始化客户端的存根
     */
    public ClientTest() {
        ManagedChannel channel = NettyChannelBuilder.forAddress(DEFAULT_HOSTNAME, DEFAULT_PORT).negotiationType(NegotiationType.PLAINTEXT).build();
        namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 注册服务
     *
     * @param hostname 主机名
     * @param ip       ip地址
     */
    public void register(String hostname, String ip) {
        RegisterRequest request = RegisterRequest.newBuilder().setHostname(hostname).setIp(ip).build();
        RegisterResponse response = namenode.register(request);
        System.out.println("客户端注册[" + hostname + ":" + ip + "]服务收到响应结果:" + response.getStatus());
    }

    /**
     * 心跳
     */
    public void heartbeat(String hostname, String ip) {
        HeartbeatRequest request = HeartbeatRequest.newBuilder().setHostname(hostname).setIp(ip).build();
        HeartbeatResponse response = namenode.heartbeat(request);
        System.out.println("客户端发送心跳[" + hostname + ":" + ip + "]收到响应结果：" + response.getStatus());
    }

    public static void main(String[] args) {
        ClientTest clientTest = new ClientTest();
        clientTest.heartbeat("stockService", "10.33.40.104");
        clientTest.register("orderService", "10.33.40.160");
    }
}
