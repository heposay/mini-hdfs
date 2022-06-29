package com.hepo.dfs.namenode.server;

import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Description: NameNode 的rpc服务接口
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:50
 *
 * @author linhaibo
 */
public class NameNodeRpcServer {

    /**
     * 负责管理元数据的组件
     */
    private FSNamesystem namesystem;

    /**
     * 负责管理datanode的组件
     */
    private DataNodeManager dataNodeManager;

    private Server server = null;

    private static final int DEFAULT_PORT = 50070;

    public NameNodeRpcServer(FSNamesystem namesystem, DataNodeManager dataNodeManager) {
        this.namesystem = namesystem;
        this.dataNodeManager = dataNodeManager;
    }


    /**
     * 启动这个Server服务
     */
    public void start() throws IOException {
        //启动rpc server 服务，并监听端口
        server = ServerBuilder.forPort(DEFAULT_PORT)
                .addService(NameNodeServiceGrpc.bindService(new NameNodeServiceImpl(namesystem, dataNodeManager)))
                .build()
                .start();

        System.out.println("NameNodeRpcServer启动，监听端口号：" + DEFAULT_PORT);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    NameNodeRpcServer.this.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
