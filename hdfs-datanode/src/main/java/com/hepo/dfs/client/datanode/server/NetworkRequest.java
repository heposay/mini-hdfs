package com.hepo.dfs.client.datanode.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

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
     * processor标识
     */
    private Integer processorId;

    /**
     * 客户端的地址
     */
    private String clientAddr;
    /**
     * 本次网络请求对应的key
     */
    private SelectionKey key;

    /**
     * 本次网络请求对应的连接
     */
    private SocketChannel channel;
    /**
     * 缓存没读取完的文件数据
     */
    private final CachedRequest cachedRequest = new CachedRequest();

    /**
     * 缓存没读取完的请求类型
     */
    private ByteBuffer cachedRequestTypeBuffer;
    /**
     * 缓存没读取完的文件名大小
     */
    private ByteBuffer cachedFilenameLengthBuffer;
    /**
     * 缓存没读取完的文件名
     */
    private ByteBuffer cachedFilenameBuffer;
    /**
     * 缓存没读取完的文件大小
     */
    private ByteBuffer cachedFileLengthBuffer;
    /**
     * 缓存没读取完的文件
     */
    private ByteBuffer cachedFileBuffer;

    public NetworkRequest(SelectionKey key, SocketChannel channel) {
        System.out.println("NetworkRequest请求初始化：" + DATA_DIR);
        this.key = key;
        this.channel = channel;
    }

    public Integer getProcessorId() {
        return processorId;
    }

    public void setProcessorId(Integer processorId) {
        this.processorId = processorId;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    /**
     * 从网络连接中读取与解析出来一个请求
     */
    public void read() {
        try {
            Integer requestType = null;
            if (cachedRequest.requestType != null) {
                requestType = cachedRequest.requestType;
            } else {
                requestType = getRequestType(channel);
            }
            if (requestType == null) {
                return;
            }
            System.out.println("从请求中解析出来请求类型：" + requestType);

            if (REQUEST_SEND_FILE.equals(requestType)) {
                handleSendFileRequest(channel);
            } else if (REQUEST_READ_FILE.equals(requestType)) {
                handleReadFileRequest(channel);
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
        if (cachedRequest.requestType != null) {
            return cachedRequest.requestType;
        }
        ByteBuffer requestTypeBuffer;
        if (cachedRequestTypeBuffer != null) {
            requestTypeBuffer = cachedRequestTypeBuffer;
        } else {
            requestTypeBuffer = ByteBuffer.allocate(4);
        }

        channel.read(requestTypeBuffer);
        if (!requestTypeBuffer.hasRemaining()) {
            requestTypeBuffer.rewind();
            requestType = requestTypeBuffer.getInt();
            cachedRequest.requestType = requestType;
        } else {
            cachedRequestTypeBuffer = requestTypeBuffer;
        }
        return requestType;
    }

    /**
     * 是否已经完成了一个请求的读取
     */
    public Boolean hasCompletedRead() {
        return cachedRequest.hasCompletedRead;
    }


    /**
     * 处理发送文件的请求
     *
     * @param channel 客户端的channel
     * @throws IOException 当channel为空的时候，就会报错
     */
    private void handleSendFileRequest(SocketChannel channel) throws IOException {
        //从请求中解析文件名
        FilenamePath filenamePath = getFileNamePath(channel);
        System.out.println("从网络请求解析出来文件名：" + filenamePath);
        if (filenamePath == null) {
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
        if (cachedFileBuffer != null) {
            fileBuffer = cachedFileBuffer;
        } else {
            fileBuffer = ByteBuffer.allocate(Math.toIntExact(fileLength));
        }
        //将数据读到fileBuffer
        channel.read(fileBuffer);
        if (!fileBuffer.hasRemaining()) {
            fileBuffer.rewind();
            cachedRequest.file = fileBuffer;
            cachedRequest.hasCompletedRead = true;
            System.out.println("本次文件上传请求读取完毕.......");
        } else {
            cachedFileBuffer = fileBuffer;
            System.out.println("本次文件上传出现拆包问题，缓存起来，下次继续读取.......");
        }
    }

    /**
     * 读取DataNode磁盘上的文件
     *
     * @param channel 客户端的channel
     */
    private void handleReadFileRequest(SocketChannel channel) throws IOException {
        //从请求中解析出文件的路径
        FilenamePath filenamePath = getFileNamePath(channel);
        System.out.println("从客户端请求中解析出文件目录名：" + filenamePath);
        if (filenamePath == null) {
            return;
        }
        cachedRequest.hasCompletedRead = true;
    }


    /**
     * 从网络请求中获取文件名
     *
     * @param channel 客户端channel
     * @return 文件名
     * @throws IOException 如果客户端连接发生断开，会抛出该异常。
     */
    private FilenamePath getFileNamePath(SocketChannel channel) throws IOException {
        //尝试从缓存中获取，如果没有，则从channel里面获取
        if (cachedRequest.filenamePath != null) {
            return cachedRequest.filenamePath;
        } else {
            FilenamePath filename = new FilenamePath();
            //获取文件名
            String relativeFilenamePath = getRelativeFilenamePath(channel);
            if (relativeFilenamePath == null) {
                return null;
            }
            filename.relativeFilenamePath = relativeFilenamePath;
            filename.absoluteFilenamePath = getAbsoluteFilenamePath(relativeFilenamePath);

            //缓存请求对象
            cachedRequest.filenamePath = filename;
            return filename;
        }
    }


    /**
     * 获取相对路径文件名
     *
     * @param channel 客户端channel
     * @return 文件名
     */
    private String getRelativeFilenamePath(SocketChannel channel) throws IOException {
        String filename = null;
        Integer filenameLength = null;
        //先读取文件名大小
        if (cachedRequest.filenameLength == null) {
            ByteBuffer filenameLengthBuffer = null;
            if (cachedFilenameLengthBuffer != null) {
                filenameLengthBuffer = cachedFilenameLengthBuffer;
            } else {
                filenameLengthBuffer = ByteBuffer.allocate(4);
            }
            channel.read(filenameLengthBuffer);
            if (!filenameLengthBuffer.hasRemaining()) {
                filenameLengthBuffer.rewind();
                filenameLength = filenameLengthBuffer.getInt();
                cachedRequest.filenameLength = filenameLength;
            } else {
                cachedFilenameLengthBuffer = filenameLengthBuffer;
                return null;
            }
        } else {
            //读取文件名
            ByteBuffer filenameBuffer;
            if (cachedFilenameBuffer != null) {
                filenameBuffer = cachedFilenameBuffer;
            } else {
                filenameBuffer = ByteBuffer.allocate(cachedRequest.filenameLength);
            }
            channel.read(filenameBuffer);
            if (!filenameBuffer.hasRemaining()) {
                filenameBuffer.rewind();
                filename = new String(filenameBuffer.array());
            } else {
                cachedFilenameBuffer = filenameBuffer;
            }
        }
        return filename;
    }

    /**
     * 获取文件在本地磁盘的绝对路径
     *
     * @param relativeFilenamePath 文件相对路径
     * @return 文件绝对路径
     */
    private String getAbsoluteFilenamePath(String relativeFilenamePath) {
        //解析文件名，然后创建目录
        String[] splitRelativeDir = relativeFilenamePath.split(StringPoolConstant.DIAGONAL);
        StringBuilder dirPath = new StringBuilder(DATA_DIR);
        for (int i = 1; i < splitRelativeDir.length - 1; i++) {
            dirPath.append(StringPoolConstant.DIAGONAL).append(splitRelativeDir[i]);
        }

        File dir = new File(dirPath.toString());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        //拼接最终的filename
        return dirPath + StringPoolConstant.DIAGONAL + splitRelativeDir[splitRelativeDir.length - 1];
    }

    /**
     * 获取文件大小长度
     *
     * @param channel 客户端channel
     * @return 文件长度
     */
    private Long getFileLength(SocketChannel channel) throws IOException {
        Long fileLength = null;
        if (cachedRequest.fileLength != null) {
            fileLength = cachedRequest.fileLength;
        } else {
            ByteBuffer fileLengthBuffer = null;
            if (cachedFileLengthBuffer != null) {
                fileLengthBuffer = cachedFileLengthBuffer;
            } else {
                fileLengthBuffer = ByteBuffer.allocate(8);
            }
            channel.read(fileLengthBuffer);
            if (!fileLengthBuffer.hasRemaining()) {
                fileLengthBuffer.rewind();
                fileLength = fileLengthBuffer.getLong();
                cachedRequest.fileLength = fileLength;
            } else {
                cachedFileLengthBuffer = fileLengthBuffer;
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
        Integer filenameLength;
        Long fileLength;
        ByteBuffer file;
        Boolean hasCompletedRead = false;
    }

    public Integer getRequestType() {
        return cachedRequest.requestType;
    }

    public FilenamePath getFileNamePath() {
        return cachedRequest.filenamePath;
    }

    public Long getFileLength() {
        return cachedRequest.fileLength;
    }

    public ByteBuffer getFile() {
        return cachedRequest.file;
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

    public String getRelativeFilenamePath() {
        return cachedRequest.filenamePath.relativeFilenamePath;
    }
    public String getAbsoluteFilenamePath() {
        return cachedRequest.filenamePath.absoluteFilenamePath;
    }

}
