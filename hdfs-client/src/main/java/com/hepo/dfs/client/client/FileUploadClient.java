package com.hepo.dfs.client.client;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.hepo.dfs.client.client.NetworkRequest.*;

/**
 * Description: 文件上传客户端
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 10:41
 *
 * @author linhaibo
 */
public class FileUploadClient {

    private NetworkManager networkManager;

    public FileUploadClient() {
        this.networkManager = new NetworkManager();
    }

    /**
     * 向服务端发送文件流
     *
     * @param fileInfo 文件信息
     * @param host     DataNode主机
     */
    public Boolean sendFile(FileInfo fileInfo, Host host, ResponseCallback callback) {
        //根据hostname来检查一下，跟对方机器的连接是否建立好了
        //如果还没有建立好，那么就直接在这里建立连接
        //建立好连接之后，就应该把连接给缓存起来，以备下次来进行使用
        if (!networkManager.maybeConnect(host.getHostname(), host.getUploadServerPort())) {
            return false;
        }
        //创建网络请求
        NetworkRequest request = createSendFileRequest(fileInfo, host, callback);
        //异步发送请求
        networkManager.sendRequestToWaitingRequests(request);
        return true;
    }

    /**
     * 创建sendFileRequest网络请求
     *
     * @param fileInfo 文件信息
     * @param host     DataNode主机信息
     * @return 网络请求对象
     */
    private NetworkRequest createSendFileRequest(FileInfo fileInfo, Host host, ResponseCallback callback) {
        NetworkRequest networkRequest = new NetworkRequest();

        ByteBuffer buffer = ByteBuffer.allocate(
                REQUEST_TYPE +
                FILENAME_LENGTH +
                fileInfo.getFileName().getBytes().length +
                FILE_LENGTH +
                Math.toIntExact(fileInfo.getFileLength()));

        buffer.putInt(REQUEST_SEND_FILE);
        buffer.putInt(fileInfo.getFileName().getBytes().length);
        buffer.put(fileInfo.getFileName().getBytes());
        buffer.putLong(fileInfo.getFileLength());
        buffer.put(fileInfo.getFile());
        buffer.rewind();

        networkRequest.setId(UUID.randomUUID().toString());
        networkRequest.setHostname(host.getHostname());
        networkRequest.setIp(host.getIp());
        networkRequest.setUploadServerPort(host.getUploadServerPort());
        networkRequest.setBuffer(buffer);
        networkRequest.setNeedResponse(false);
        networkRequest.setCallback(callback);
        return networkRequest;
    }

    /**
     * 向服务端获取文件
     *
     * @param host     DataNode机器信息
     * @param filename 文件名
     * @return 网络请求对象
     */
    public byte[] readFile(Host host, String filename, Boolean retry) throws Exception {
        //与DataNode尝试建立连接
        if (!networkManager.maybeConnect(host.getHostname(), host.getUploadServerPort())) {
            if (retry) {
                throw new Exception();
            }
        }

        //创建readFileRequest请求对象
        NetworkRequest request = crewateReadFileRequest(host, filename, null);
        networkManager.sendRequestToWaitingRequests(request);
        NetworkResponse response = networkManager.waitResponse(request.getId());
        if (response.isError()) {
            if (retry) {
                throw new Exception();
            }
        }
        return response.getBuffer().array();
    }

    /**
     * 创建ReadFileRequest网络请求
     *
     * @param host     DataNode机器信息
     * @param filename 文件名
     * @param callback 回调函数
     */
    private NetworkRequest crewateReadFileRequest(Host host, String filename, ResponseCallback callback) {
        NetworkRequest request = new NetworkRequest();

        byte[] filenameBytes = filename.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(REQUEST_TYPE + FILENAME_LENGTH + filenameBytes.length);
        buffer.putInt(REQUEST_TYPE);
        buffer.putInt(filenameBytes.length);
        buffer.put(filenameBytes);
        buffer.rewind();

        request.setId(UUID.randomUUID().toString());
        request.setHostname(host.getHostname());
        request.setIp(host.getIp());
        request.setUploadServerPort(host.getUploadServerPort());
        request.setBuffer(buffer);
        request.setNeedResponse(true);
        request.setCallback(callback);
        request.setRequestType(NetworkRequest.REQUEST_READ_FILE);

        return request;
    }

}
