package com.hepo.dfs.namenode.rpc.server.test;

import com.hepo.dfs.namenode.rpc.model.HeartbeatRequest;
import com.hepo.dfs.namenode.rpc.model.HeartbeatResponse;
import com.hepo.dfs.namenode.rpc.model.RegisterRequest;
import com.hepo.dfs.namenode.rpc.model.RegisterResponse;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

/**
 * Description:
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-29 09:01
 *
 * @author linhaibo
 */
public class ServerTest {
    private Server server;

    public static void main(String[] args) throws Exception {
        final ServerTest serverTest = new ServerTest();
        serverTest.start();
        serverTest.blockUntilShutdown();
    }

    public void start() throws Exception {
        int port = 50070;
        server = ServerBuilder.forPort(port).addService(new NameNodeServiceImpl()).build().start();

        //注册关闭JVM的方法
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                ServerTest.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    /**
     * 实现类
     */
    static class NameNodeServiceImpl extends NameNodeServiceGrpc.NameNodeServiceImplBase {
        @Override
        public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
            RegisterResponse response = RegisterResponse.newBuilder().setStatus(100).build();
            System.out.println("服务端创建目录成功，参数为：" + request.getHostname() + ":" + request.getIp());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            HeartbeatResponse response = HeartbeatResponse.newBuilder().setStatus(100).build();
            System.out.println("服务端接收客户端心跳成功，参数为：" + request.getHostname() + ":" +request.getIp());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

}
