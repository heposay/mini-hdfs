package com.hepo.dfs.client.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.hepo.dfs.namenode.rpc.model.*;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import static com.hepo.dfs.client.datanode.server.DataNodeConfig.*;

/**
 * Description: 负责跟一组NameNode中的某一个进行通信的线程组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-05-06 14:25
 *
 * @author linhaibo
 */
public class NameNodeRpcClient {


    /**
     * namenode的客户端
     */
    private final NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    /**
     * 构造方法
     */
    public NameNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 向自己负责通信的那个NameNode进行注册
     */
    public Boolean register(){
        // 发送rpc接口调用请求到NameNode去进行注册
        // 我们写代码的时候，主要是在本地来运行和测试，有一些ip和hostname，就直接在代码里写死了
        // 大家后面自己可以留空做一些完善，你可以加一些配置文件读取的代码
        // 通过RPC接口发送到NameNode他的注册接口上去
        RegisterRequest registerRequest = RegisterRequest.newBuilder()
                .setIp(DATANAME_IP)
                .setHostname(DATANAME_HONENAME)
                .setUploadServerPort(FILE_UPLOAD_SERVER_PORT)
                .build();
        RegisterResponse response = namenode.register(registerRequest);
        if (response.getStatus() == 1) {
            return true;
        }else {
            return false;
        }
    }


    /**
     * 通知Master节点自己收到了一个文件的副本
     * @param filename 文件名
     */
    public void informReplicaReceived(String filename) {
        InformReplicaReceivedRequest request = InformReplicaReceivedRequest.newBuilder()
                .setIp(DATANAME_IP)
                .setHostname(DATANAME_HONENAME)
                .setFilename(filename)
                .build();
        namenode.informReplicaReceived(request);
    }

    /**
     * 上传全量存储信息
     * @param storageInfo 存储信息
     */
    public void reportCompleteStorageInfo(StorageInfo storageInfo) {
        ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest.newBuilder()
                .setIp(DATANAME_IP)
                .setHostname(DATANAME_HONENAME)
                .setFilenames(JSONArray.toJSONString(storageInfo.getFilenames()))
                .setStorageDataSize(storageInfo.getStorageSize())
                .build();
        namenode.reportCompleteStorageInfo(request);
    }

    /**
     * 发送心跳
     */
    public HeartbeatResponse heartbeat() {
        // 通过RPC接口发送到NameNode他的注册接口上去
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setIp(DATANAME_IP)
                .setHostname(DATANAME_HONENAME)
                .build();
        return namenode.heartbeat(request);
    }
}
