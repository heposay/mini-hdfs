package com.hepo.dfs.namenode.server;

import com.hepo.dfs.namenode.rpc.model.HeartbeatRequest;
import com.hepo.dfs.namenode.rpc.model.HeartbeatResponse;
import com.hepo.dfs.namenode.rpc.model.RegisterRequest;
import com.hepo.dfs.namenode.rpc.model.RegisterResponse;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Description: NameNode的服务接口
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-07 16:22
 *
 * @author linhaibo
 */
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {


    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;

    /**
     * 负责管理元数据的核心组件
     */
    private FSNamesystem namesystem;
    /**
     * 负责管理集群中所有的datanode的组件
     */
    private DataNodeManager datanodeManager;

    public NameNodeServiceImpl(FSNamesystem namesystem, DataNodeManager datanodeManager) {
        this.namesystem = namesystem;
        this.datanodeManager = datanodeManager;
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     * @return 是否创建成功
     * @throws Exception
     */
    public Boolean mkdir(String path) throws Exception {
        return this.namesystem.mkdir(path);
    }

    /**
     * datanode进行注册
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        datanodeManager.register(request.getIp(), request.getHostname());
        System.out.println("收到客户端["+request.getIp() + ":" + request.getHostname() + "]的注册信息");
        RegisterResponse response = RegisterResponse.newBuilder()
                .setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 对datanode进行心跳检测
     * @param request
     * @param responseObserver
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        datanodeManager.heartbeat(request.getIp(), request.getHostname());
        System.out.println("收到客户端["+request.getIp() + ":" + request.getHostname() + "]的心跳信息");
        HeartbeatResponse response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }
}
