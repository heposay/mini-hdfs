package com.hepo.dfs.client.client;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hepo.dfs.namenode.rpc.model.*;
import com.hepo.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import static com.hepo.dfs.client.client.ClientConfig.NAMENODE_HOSTNAME;
import static com.hepo.dfs.client.client.ClientConfig.NAMENODE_PORT;

/**
 * 文件系统客户端的实现类
 *
 * @author zhonghuashishan
 */
public class FileSystemImpl implements FileSystem {

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
        ManagedChannel channel = NettyChannelBuilder.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
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
        MkdirRequest request = MkdirRequest.newBuilder().setPath(path).build();
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
     * 1.用filename发送一个RPC接口到master节点，master节点会进行查重，如果已经有了，则不让创建
     * 2.master节点此时会计算存储量最少的两个数据节点，尽可能分配数据节点的时候，保证让每个数据及诶单方的数据量都是比较均衡的，将数据节点返回给客户端
     * 3.假如某些节点可能上传失败，需要一个容错机制，重新向master节点请求其他数据节点，再次建立连接发送请求
     * 4.该上传文件是基于异步来实现的，需要传入回调函数callback
     *
     * @param fileInfo 文件信息
     * @return 是否上传成功
     */
    @Override
    public Boolean upload(FileInfo fileInfo, ResponseCallback callback) {
        if (!createFile(fileInfo.getFileName())) {
            return false;
        }
        JSONArray datanodes = JSONArray.parseArray(allocateDataNodes(fileInfo.getFileName(), fileInfo.getFileLength()));
        for (int i = 0; i < datanodes.size(); i++) {
            Host host = getHost(datanodes.getJSONObject(i));
            if (!fileUploadClient.sendFile(fileInfo, host, callback)) {
                host = reallocateDataNode(fileInfo, host.getId());
                fileUploadClient.sendFile(fileInfo, host, null);
            }
        }
        return true;
    }


    /**
     * 下载文件
     * 1.用filename发送一个RPC接口到master节点，master根据filename找到该文件所在的副本机器，并返回给客户端
     * 2.假如某些节点可能下载失败，需要一个容错机制，重新向master节点请求其他数据节点，再次建立连接发送请求
     * 3.该下载文件接口是基于同步来做的，如果文件过大，可能会导致超时（这里需要修改一下超时机制）
     *
     * @param filename 文件名（包含文件相对目录）
     * @return 文件二进制数据流
     */
    @Override
    public byte[] download(String filename) {
        Host datanode = chooseDataNodeFromReplicas(filename, "");
        byte[] file = null;
        try {
            file = fileUploadClient.readFile(datanode, filename, true);
        } catch (Exception e) {
            datanode = chooseDataNodeFromReplicas(filename, datanode.getId());
            try {
                file = fileUploadClient.readFile(datanode, filename, false);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return file;
    }

    /**
     * 获取文件的某个副本所在的机器
     *
     * @param filename           文件名
     * @param excludedDataNodeId 要的排除的节点
     * @return DataNode所在的机器
     */
    private Host chooseDataNodeFromReplicas(String filename, String excludedDataNodeId) {
        ChooseDataNodeFromReplicasRequest request = ChooseDataNodeFromReplicasRequest.newBuilder().setFilename(filename).setExcludedDataNodeId(excludedDataNodeId).build();
        ChooseDataNodeFromReplicasResponse response = namenode.chooseDataNodeFromReplicas(request);
        return getHost(JSONObject.parseObject(response.getDataNodeInfo()));
    }


    /**
     * 为上传文件分配DataNode数据节点集合
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

    /**
     * 为上传文件重新分配DataNode数据节点集合，需要排除故障的节点信息
     *
     * @param fileInfo           文件信息
     * @param excludedDataNodeId 要排除的节点信息
     * @return datanode节点的JSON串
     */
    private Host reallocateDataNode(FileInfo fileInfo, String excludedDataNodeId) {
        ReallocateDataNodeRequest request = ReallocateDataNodeRequest.newBuilder().setFilename(fileInfo.getFileName()).setFileSize(fileInfo.getFileLength()).setExcludedDataNodeId(excludedDataNodeId).build();
        ReallocateDataNodeResponse response = namenode.reallocateDataNode(request);
        return getHost(JSONObject.parseObject(response.getDatanode()));
    }


    /**
     * 创建文件
     *
     * @param filename 文件名称
     * @return 是否创建成功
     */
    private Boolean createFile(String filename) {
        CreateFileRequest request = CreateFileRequest.newBuilder().setFilename(filename).build();
        CreateFileResponse response = namenode.create(request);
        if (response.getStatus() == 1) {
            return true;
        }
        return false;
    }

    /**
     * 重新上传文件，需要排除上次的故障节点
     *
     * @param fileInfo     文件信息
     * @param excludedHost 要排除的数据节点
     * @return 是否上传成功
     */
    @Override
    public Boolean retryUpload(FileInfo fileInfo, Host excludedHost) {
        Host host = reallocateDataNode(fileInfo, excludedHost.getId());
        fileUploadClient.sendFile(fileInfo, host, null);
        return true;
    }


    /**
     * 获取DataNode数据节点对象
     */
    private Host getHost(JSONObject datanode) {
        Host host = new Host();
        host.setHostname(datanode.getString("hostname"));
        host.setIp(datanode.getString("ip"));
        host.setUploadServerPort(datanode.getInteger("uploadServerPort"));
        return host;
    }
}
