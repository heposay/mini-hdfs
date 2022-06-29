package com.hepo.dfs.client.client;


import com.hepo.dfs.namenode.rpc.model.MkdirRequest;
import com.hepo.dfs.namenode.rpc.model.MkdirResponse;
import com.hepo.dfs.namenode.rpc.model.ShutdownRequest;
import com.hepo.dfs.namenode.rpc.model.ShutdownResponse;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * 文件系统客户端的实现类
 *
 * @author zhonghuashishan
 */
public class FileSystemImpl implements FileSystem {

    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public FileSystemImpl() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 创建目录
     */
    @Override
    public void mkdir(String path) {
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(path)
                .build();

        MkdirResponse response = namenode.mkdir(request);

        System.out.println("创建目录的响应：" + response.getStatus());
    }

    @Override
    public void shutdown() {
        ShutdownRequest request = ShutdownRequest.newBuilder().setCode(1).build();
        ShutdownResponse response = namenode.shutdown(request);
        System.out.println("关闭namenode服务器响应：" + response.getStatus());
    }

}
