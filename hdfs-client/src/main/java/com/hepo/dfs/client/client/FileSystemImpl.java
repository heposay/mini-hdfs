package com.hepo.dfs.client.client;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hepo.dfs.namenode.rpc.model.*;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;

/**
 * 文件系统客户端的实现类
 *
 * @author zhonghuashishan
 */
public class FileSystemImpl implements FileSystem {

    /**
     * 主机名
     */
    private static final String NAMENODE_HOSTNAME = "localhost";
    /**
     * 端口号
     */
    private static final Integer NAMENODE_PORT = 50070;

    /**
     * namenode服务端，用于发送请求，基于RPC协议实现
     */
    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    /**
     * 文件上传客户端
     */
    private FileUploadClient fileUploadClient;

    public FileSystemImpl() {
        //初始化namenode组件
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        this.fileUploadClient = new FileUploadClient();
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

    /**
     * 关闭namenode服务器
     */
    @Override
    public void shutdown() {
        ShutdownRequest request = ShutdownRequest.newBuilder().setCode(1).build();
        ShutdownResponse response = namenode.shutdown(request);
        System.out.println("关闭namenode服务器响应：" + response.getStatus());
    }

    /**
     * 上传文件
     *
     * @param file     文件字节流
     * @param filename 文件名
     * @param fileSize 文件大小
     */
    @Override
    public Boolean upload(byte[] file, String filename, long fileSize) {
        //用filename发送一个RPC接口调用master节点
        //master节点会进行查重，如果已经有了，则不让创建
        if (!createFile(filename)) {
            return false;
        }

        //找master节点要多个数据节点的地址
        //考虑自己上传几个副本，找到副本对应的节点地址
        //尽可能分配数据节点的时候，保证让每个数据及诶单方的数据量都是比较均衡的
        String datanodesJson = allocateDataNodes(filename, fileSize);
        System.out.println("获取分配的datanode节点：" + datanodesJson);

        //依次把文件的副本上传到各个数据节点去。
        //此时有可能某些节点上传失败，需要有一个容错的机制
        JSONArray datanodes = JSONArray.parseArray(datanodesJson);
        for (int i = 0; i < datanodes.size(); i++) {
            JSONObject datanode = datanodes.getJSONObject(i);
            String hostname = datanode.getString("hostname");
            int uploadServerPort = datanode.getInteger("uplaodServerPort");
            fileUploadClient.sendFile(hostname, uploadServerPort, file, filename, fileSize);
        }
        return true;
    }

    @Override
    public byte[] download(String filename) throws IOException {
        //1.调用Namenode的接口，获取该文件副本所在的DataNode
        JSONObject datanode =getDataNodeForFile(filename);
        System.out.println("获取要下载的DataNode节点：" + datanode);
        //2.打开DataNode的网络连接，发送文件名过去
        //3.尝试从连接读取DataNode发送过来的文件流数据
        String hostname = datanode.getString("hostname");
        Integer uploadServerPort = datanode.getInteger("uplaodServerPort");
        return fileUploadClient.readFile(hostname, uploadServerPort, filename);
    }

    /**
     * 获取文件的某个副本所在的机器
     * @param filename 文件名
     * @return DataNode所在的机器
     */
    private JSONObject getDataNodeForFile(String filename) {
        GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                .setFilename(filename)
                .build();
        GetDataNodeForFileResponse response = namenode.getDataNodeForFile(request);
        return JSONObject.parseObject(response.getDataNodeInfo());
    }


    /**
     * 创建文件
     *
     * @param filename 文件名称
     * @return 是否成功
     */
    private Boolean createFile(String filename) {
        CreateFileRequest request = CreateFileRequest.newBuilder().setFilename(filename).build();
        CreateFileResponse response = namenode.create(request);
        System.out.println("向NameNode发送创建文件的请求，响应结果：" + response.getStatus());
        if (response.getStatus() == 1) {
            return true;
        }
        return false;
    }

    /**
     * 获取分配好的datanode节点
     *
     * @param filename 文件名
     * @param fileSize 文件大小
     * @return datanode节点的JSON串
     */
    private String allocateDataNodes(String filename, long fileSize) {
        AllocateDataNodesRequest request = AllocateDataNodesRequest.newBuilder().setFilename(filename).setFileSize(fileSize).build();
        AllocateDataNodesResponse response = namenode.allocateDataNodes(request);
        return response.getDatanodes();
    }
}
