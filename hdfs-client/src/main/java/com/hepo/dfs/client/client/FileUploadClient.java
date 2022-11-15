package com.hepo.dfs.client.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.UUID;

/**
 * Description: 文件上传客户端
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 10:41
 *
 * @author linhaibo
 */
public class FileUploadClient {

    public static final Integer REQUEST_TYPE = 4;
    public static final Integer FILENAME_LENGTH = 4;
    public static final Integer FILE_LENGTH = 8;
    public static final Integer SEND_FILE = 1;
    public static final Integer READ_FILE = 2;

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
        networkManager.sendRequest(request);
        return true;
    }

    /**
     * 创建网络请求
     *
     * @param fileInfo 文件信息
     * @param host     DataNode主机信息
     */
    private NetworkRequest createSendFileRequest(FileInfo fileInfo, Host host, ResponseCallback callback) {
        NetworkRequest networkRequest = new NetworkRequest();

        ByteBuffer buffer = ByteBuffer.allocate(
                REQUEST_TYPE +
                        FILENAME_LENGTH +
                        fileInfo.getFileName().getBytes().length +
                        FILE_LENGTH +
                        Math.toIntExact(fileInfo.getFileLength()));

        buffer.putInt(SEND_FILE);
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
        networkRequest.setSendTime(System.currentTimeMillis());
        networkRequest.setCallback(callback);
        return networkRequest;
    }

    /**
     * 向服务端获取文件
     *
     * @param hostname         服务端ip地址
     * @param uploadServerPort 服务端端口号
     * @param filename         文件名
     */
    public byte[] readFile(String hostname, int uploadServerPort, String filename) {
        byte[] file = null;
        SocketChannel channel = null;
        Selector selector = null;

        ByteBuffer fileLengthBuffer = null;
        ByteBuffer fileBuffer = null;
        Long fileLength = null;

        try {
            //与服务端DataNode建立短连接，发送完一个文件立刻释放网络连接
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostname, uploadServerPort));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean reading = true;
            while (reading) {
                //轮询selector
                selector.select();

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();
                    //判断key的类型
                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();
                        if (channel.isConnectionPending()) {
                            channel.finishConnect(); //把三次握手做完，建立好TCP连接

                            //封装文件的请求数据
                            byte[] filenameBytes = filename.getBytes();

                            ByteBuffer readFileRequest = ByteBuffer.allocate(8 + filenameBytes.length);
                            readFileRequest.putInt(READ_FILE);
                            //Int对应了4个字节，放到缓冲区里去
                            readFileRequest.putInt(filenameBytes.length);
                            //把真正的文件名给放入进去
                            readFileRequest.put(filenameBytes);

                            //每次write buffer之前，一定要flip。
                            readFileRequest.flip();
                            //将缓冲区数据写到channel
                            channel.write(readFileRequest);

                            //重新监听读事件
                            channel.register(selector, SelectionKey.OP_READ);
                        }
                    } else if (key.isReadable()) {
                        //读取服务端发回来的响应
                        channel = (SocketChannel) key.channel();
                        if (fileLength == null) {
                            if (fileLengthBuffer == null) {
                                fileLengthBuffer = ByteBuffer.allocate(8);
                            }
                            channel.read(fileLengthBuffer);

                            if (!fileLengthBuffer.hasRemaining()) {
                                fileLengthBuffer.rewind();
                                fileLength = fileLengthBuffer.getLong();
                            }
                        } else {
                            if (fileBuffer == null) {
                                fileBuffer = ByteBuffer.allocate(Math.toIntExact(fileLength));
                            }
                            channel.read(fileBuffer);
                            if (!fileBuffer.hasRemaining()) {
                                fileBuffer.rewind();
                                file = fileBuffer.array();
                                reading = false;
                            }
                        }

                    }
                }
            }
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }

}
