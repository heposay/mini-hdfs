package com.hepo.dfs.client.datanode.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.DATA_DIR;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 00:45
 *
 * @author linhaibo
 */
public class NetworkRequest {
    private final static Integer REQUEST_SEND_FILE = 1;
    private final static Integer REQUEST_READ_FILE = 2;

    /**
     * 客户端的地址
     */
    private String clientAddr;
    /**
     * 本次网络请求对应的key
     */
    private SelectionKey selectionKey;

    /**
     * 本次网络请求对应的连接
     */
    private SocketChannel channel;
    /**
     * 缓存没读取完的文件数据
     */
    private final Map<String, CachedRequest> cacheRequests = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的请求类型
     */
    private final Map<String, ByteBuffer> requestTypeByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件名大小
     */
    private final Map<String, ByteBuffer> filenameLengthByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件名
     */
    private final Map<String, ByteBuffer> filenameByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件大小
     */
    private final Map<String, ByteBuffer> fileLengthByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件
     */
    private final Map<String, ByteBuffer> fileByClient = new ConcurrentHashMap<>();

    public NetworkRequest(SelectionKey selectionKey, SocketChannel channel) {
        this.selectionKey = selectionKey;
        this.channel = channel;
        try {
            this.clientAddr = channel.getRemoteAddress().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从网络连接中读取与解析出来一个请求
     */
    public void read() {
        try {
            Integer requestType = null;
            if (cacheRequests.containsKey(clientAddr)) {
                requestType = getCachedRequest(clientAddr).requestType;
            } else {
                requestType = getRequestType(channel);
            }
            if (requestType == null) {
                return;
            }
            System.out.println("从请求中解析出来请求类型：" + requestType);

            if (REQUEST_SEND_FILE.equals(requestType)) {
                handleSendFileRequest(channel, selectionKey);
            } else if (REQUEST_READ_FILE.equals(requestType)) {
                handleReadFileRequest(channel, selectionKey);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取本次请求的类型
     *
     * @param channel 客户端的channel
     * @return 请求的类型
     */
    private Integer getRequestType(SocketChannel channel) throws IOException {
        Integer requestType = null;
        if (getCachedRequest(clientAddr).requestType != null) {
            return getCachedRequest(clientAddr).requestType;
        }
        ByteBuffer requestTypeBuffer;
        if (requestTypeByClient.containsKey(clientAddr)) {
            requestTypeBuffer = requestTypeByClient.get(clientAddr);
        } else {
            requestTypeBuffer = ByteBuffer.allocate(4);
        }

        channel.read(requestTypeBuffer);
        if (!requestTypeBuffer.hasRemaining()) {
            requestTypeBuffer.rewind();
            requestType = requestTypeBuffer.getInt();

            requestTypeByClient.remove(clientAddr);
            CachedRequest cachedRequest = getCachedRequest(clientAddr);
            cachedRequest.requestType = requestType;
        } else {
            requestTypeByClient.put(clientAddr, requestTypeBuffer);
        }
        return requestType;
    }

    /**
     * 获取缓存的请求
     *
     * @param clientAddr
     * @return
     */
    private CachedRequest getCachedRequest(String clientAddr) {
        CachedRequest cachedRequest = cacheRequests.get(clientAddr);
        if (cachedRequest == null) {
            cachedRequest = new CachedRequest();
            cacheRequests.put(clientAddr, cachedRequest);
        }
        return cachedRequest;
    }

    /**
     * 是否已经完成了一个请求的读取
     */
    public Boolean hasCompletedRead() {
        return false;
    }


    /**
     * 处理发送文件的请求
     *
     * @param channel 客户端的channel
     * @param key     多路复用的key
     * @throws IOException
     */
    private void handleSendFileRequest(SocketChannel channel, SelectionKey key) throws IOException {
        //从请求中解析文件名
        FilenamePath filename = getFileName(channel);
        System.out.println("从网络请求解析出来文件名：" + filename);
        if (filename == null) {
            return;
        }

        //从请求中解析文件大小
        Long fileLength = getFileLength(channel);
        System.out.println("从网络请求解析出来文件的文件大小：" + fileLength);
        if (fileLength == null) {
            return;
        }

        //循环不断的从channel里读取数据，并写入磁盘文件
        ByteBuffer fileBuffer;
        if (fileByClient.containsKey(clientAddr)) {
            fileBuffer = fileByClient.get(clientAddr);
        } else {
            fileBuffer = ByteBuffer.allocate(Math.toIntExact(fileLength));
        }
        //将数据读到fileBuffer
        channel.read(fileBuffer);
        if (!fileBuffer.hasRemaining()) {
            fileBuffer.rewind();
            getCachedRequest(clientAddr).file = fileBuffer;
            getCachedRequest(clientAddr).hasCompletedRead = true;
            fileByClient.remove(clientAddr);
            System.out.println("本次文件上传请求读取完毕.......");
        } else {
            fileByClient.put(clientAddr, fileBuffer);
            System.out.println("本次文件上传出现拆包问题，缓存起来，下次继续读取.......");
        }

    }

    /**
     * 读取DataNode磁盘上的文件
     *
     * @param channel 客户端的channel
     * @param key     路由key
     */
    private void handleReadFileRequest(SocketChannel channel, SelectionKey key) throws IOException {
        String clientAddr = channel.getRemoteAddress().toString();

        //从请求中解析出文件的路径
        FilenamePath filenamePath = getFileName(channel);
        System.out.println("从客户端请求中解析出文件目录名：" + filenamePath);
        if (filenamePath == null) {
            return;
        }

        File file = new File(filenamePath.absoluteFilenamePath);
        long fileLength = file.length();

        FileInputStream fis = new FileInputStream(file);
        FileChannel fileChannel = fis.getChannel();

        ByteBuffer buffer = ByteBuffer.allocate(8 + (int) fileLength);
        //将文件长度也写到缓冲区中，方便后面客户端处理拆包问题
        buffer.putLong(fileLength);
        int hasReadFileLength = fileChannel.read(buffer);
        buffer.rewind();
        channel.write(buffer);
        buffer.clear();

        fis.close();
        fileChannel.close();

        // 判断一下，如果已经读取完毕，就返回一个成功给客户端
        if (hasReadFileLength == fileLength) {
            System.out.println("文件发送完毕，给客户端: " + clientAddr);
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);

        }
    }


    /**
     * 从网络请求中获取文件名
     *
     * @param channel 客户端channel
     * @return 文件名
     * @throws IOException 如果客户端连接发生断开，会抛出该异常。
     */
    private FilenamePath getFileName(SocketChannel channel) throws IOException {
        FilenamePath filename = new FilenamePath();
        String clientAddr = channel.getRemoteAddress().toString();
        //尝试从缓存中获取，如果没有，则从channel里面获取
        if (cacheRequests.get(clientAddr).filenamePath != null) {
            filename = cacheRequests.get(clientAddr).filenamePath;
        } else {
            //获取文件名
            String relativeFilenamePath = getRelativeFilenamePath(channel);
            if (relativeFilenamePath == null) {
                return null;
            }
            filename.relativeFilenamePath = relativeFilenamePath;
            filename.absoluteFilenamePath = getAbsoluteFilenamePath(relativeFilenamePath);

            //缓存请求对象
            CachedRequest cachedRequest = getCachedRequest(clientAddr);
            cachedRequest.filenamePath = filename;
        }
        return filename;
    }


    /**
     * 获取相对路径文件名
     *
     * @param channel 客户端channel
     * @return 文件名
     */
    private String getRelativeFilenamePath(SocketChannel channel) throws IOException {
        String clientAddr = channel.getRemoteAddress().toString();
        String filename = null;
        //读取文件名
        ByteBuffer filenameBuffer;
        if (filenameByClient.containsKey(clientAddr)) {
            filenameBuffer = filenameByClient.get(clientAddr);
            channel.read(filenameBuffer);
            if (!filenameBuffer.hasRemaining()) {
                filenameBuffer.rewind();
                filename = new String(filenameBuffer.array());
                filenameByClient.remove(clientAddr);
            } else {
                filenameByClient.put(clientAddr, filenameBuffer);
            }
        } else {
            //读取文件的大小
            ByteBuffer filenameLengthBuffer;
            //读取文件名大小
            if (fileLengthByClient.containsKey(clientAddr)) {
                filenameLengthBuffer = fileLengthByClient.get(clientAddr);
            } else {
                filenameLengthBuffer = ByteBuffer.allocate(4);
            }
            channel.read(filenameLengthBuffer);
            if (!filenameLengthBuffer.hasRemaining()) {
                filenameLengthBuffer.rewind();
                //先读取4个字节的文件名长度
                int filenameLength = filenameLengthBuffer.getInt();
                filenameLengthByClient.remove(clientAddr);
                //读取文件名
                filenameBuffer = ByteBuffer.allocate(filenameLength);
                channel.read(filenameBuffer);
                if (!filenameBuffer.hasRemaining()) {
                    filenameBuffer.rewind();
                    filename = new String(filenameBuffer.array());
                } else {
                    filenameByClient.put(clientAddr, filenameBuffer);
                }
            } else {
                filenameLengthByClient.put(clientAddr, filenameLengthBuffer);
            }
        }
        return filename;
    }

    /**
     * 获取文件在本地磁盘的绝对路径
     *
     * @param relativeFilenamePath 文件相对路径
     * @return
     */
    private String getAbsoluteFilenamePath(String relativeFilenamePath) {
        //解析文件名，然后创建目录
        String[] splitRelativeDir = relativeFilenamePath.split("/");
        StringBuilder dirPath = new StringBuilder(DATA_DIR);
        for (int i = 1; i < splitRelativeDir.length - 1; i++) {
            dirPath.append("/").append(splitRelativeDir[i]);
        }

        File dir = new File(dirPath.toString());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        //拼接最终的filename
        return dirPath + "/" + splitRelativeDir[splitRelativeDir.length - 1];
    }

    /**
     * 获取文件大小长度
     *
     * @param channel 客户端channel
     * @return 文件长度
     */
    private Long getFileLength(SocketChannel channel) throws IOException {
        long fileLength = 0L;
        String clientAddr = channel.getRemoteAddress().toString();
        if (getCachedRequest(clientAddr).fileLength != null) {
            fileLength = getCachedRequest(clientAddr).fileLength;
        } else {
            ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
            channel.read(imageLengthBuffer);
            if (!imageLengthBuffer.hasRemaining()) {
                imageLengthBuffer.rewind();
                fileLength = imageLengthBuffer.getLong();
            }
        }
        return fileLength;
    }


    /**
     * 请求缓存，用于解决拆包问题
     */
    static class CachedRequest {
        Integer requestType;
        FilenamePath filenamePath;
        Long fileLength;
        ByteBuffer file;
        Boolean hasCompletedRead = false;

    }

    /**
     * 文件名目录
     */
    static class FilenamePath {
        //相对路径名
        private String relativeFilenamePath;
        //绝对路径名
        private String absoluteFilenamePath;

        @Override
        public String toString() {
            return "FilenamePath{" + "relativeFilenamePath='" + relativeFilenamePath + '\'' + ", absoluteFilenamePath='" + absoluteFilenamePath + '\'' + '}';
        }
    }

}
