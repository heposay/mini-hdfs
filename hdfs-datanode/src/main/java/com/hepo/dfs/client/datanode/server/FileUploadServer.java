package com.hepo.dfs.client.datanode.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.*;

/**
 * Description:文件上传服务端
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 10:57
 *
 * @author linhaibo
 */
public class FileUploadServer extends Thread {

    private final static Integer SEND_FILE = 1;
    private final static Integer READ_FILE = 2;
    private final static Integer FILE_BUFFER_SIZE = 10 * 1024;

    /**
     * IO多路复用用器，负责监听多个连接请求
     */
    private Selector selector;
    /**
     * 存放处理请求的队列
     */
    private final List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();
    /**
     * 缓存没读取完的文件数据
     */
    private final Map<String, CachedRequest> cacheRequests = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的请求类型
     */
    private Map<String, ByteBuffer> requestTypeByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件名大小
     */
    private Map<String, ByteBuffer> filenameLengthByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件名
     */
    private Map<String, ByteBuffer> filenameByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件大小
     */
    private Map<String, ByteBuffer> fileLengthByClient = new ConcurrentHashMap<>();
    /**
     * 缓存没读取完的文件
     */
    private Map<String, ByteBuffer> fileByClient = new ConcurrentHashMap<>();

    /**
     * 与namenode进行通信的客户端
     */
    private NameNodeRpcClient nameNodeRpcClient;

    private String dataDir = DATA_DIR;


    static class CachedRequest {
        Integer requestType;
        FilenamePath filenamePath;
        Long fileLength;
        Long hasReadFileLength;

        public CachedRequest() {
        }

        public CachedRequest(FilenamePath filenamePath, Long fileLength, Long hasReadFileLength) {
            this.filenamePath = filenamePath;
            this.fileLength = fileLength;
            this.hasReadFileLength = hasReadFileLength;
        }

        public Integer getRequestType() {
            return requestType;
        }

        public void setRequestType(Integer requestType) {
            this.requestType = requestType;
        }

        public FilenamePath getFilenamePath() {
            return filenamePath;
        }

        public void setFilenamePath(FilenamePath filenamePath) {
            this.filenamePath = filenamePath;
        }

        public Long getFileLength() {
            return fileLength;
        }

        public void setFileLength(Long fileLength) {
            this.fileLength = fileLength;
        }

        public Long getHasReadFileLength() {
            return hasReadFileLength;
        }

        public void setHasReadFileLength(Long hasReadFileLength) {
            this.hasReadFileLength = hasReadFileLength;
        }

        @Override
        public String toString() {
            return "CachedRequest{" +
                    "requestType=" + requestType +
                    ", filenamePath=" + filenamePath +
                    ", fileLength=" + fileLength +
                    ", hasReadFileLength=" + hasReadFileLength +
                    '}';
        }
    }

    /**
     * 文件名目录
     */
    static class FilenamePath {
        //相对路径名
        private String relativeFilenamePath;
        //绝对路径名
        private String absoluteFilenamePath;

        public FilenamePath() {
        }

        public FilenamePath(String relativeFilenamePath, String absoluteFilenamePath) {
            this.relativeFilenamePath = relativeFilenamePath;
            this.absoluteFilenamePath = absoluteFilenamePath;
        }

        public String getRelativeFilenamePath() {
            return relativeFilenamePath;
        }

        public void setRelativeFilenamePath(String relativeFilenamePath) {
            this.relativeFilenamePath = relativeFilenamePath;
        }

        public String getAbsoluteFilenamePath() {
            return absoluteFilenamePath;
        }

        public void setAbsoluteFilenamePath(String absoluteFilenamePath) {
            this.absoluteFilenamePath = absoluteFilenamePath;
        }

        @Override
        public String toString() {
            return "FilenamePath{" + "relativeFilenamePath='" + relativeFilenamePath + '\'' + ", absoluteFilenamePath='" + absoluteFilenamePath + '\'' + '}';
        }
    }

    /**
     * NIOServer的初始化，监听端口、队列初始化、线程初始化
     *
     * @param nameNodeRpcClient namenode的客户端
     */
    public FileUploadServer(NameNodeRpcClient nameNodeRpcClient) {
        ServerSocketChannel ssc;
        try {
            this.nameNodeRpcClient = nameNodeRpcClient;

            selector = Selector.open();

            ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            ssc.socket().bind(new InetSocketAddress(FILE_UPLOAD_SERVER_PORT), 100);
            ssc.register(selector, SelectionKey.OP_ACCEPT);

            for (int i = 0; i < FILE_UPLOAD_SERVER_WORKER_SIZE; i++) {
                queues.add(new LinkedBlockingQueue<>());
            }
            for (int i = 0; i < FILE_UPLOAD_SERVER_WORKER_SIZE; i++) {
                //3个worker处理线程
                new Worker(queues.get(i)).start();
            }

            System.out.println("FileUploadServer已经启动，监听端口号：" + FILE_UPLOAD_SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        while (true) {
            try {
                //轮询select
                selector.select();
                //主线程负责转发客户端发送过来的请求
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();
                    handleEvents(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 处理请求分发
     * 主线程负责将请求放到对应的queue，然后worker线程负责从queue获取请求，并处理
     * 所以这里性能非常高
     */
    private void handleEvents(SelectionKey key) throws IOException {
        SocketChannel channel = null;
        try {
            if (key.isAcceptable()) {

                ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                channel = ssc.accept();
                if (channel != null) {
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                }
            } else if (key.isReadable()) {
                //轮询查出每一个客户端，然后将客户端的hashcode与队列的大小进行取模，然后将key放到对应的队列中
                channel = (SocketChannel) key.channel();
                String clientAddr = channel.getRemoteAddress().toString();
                int queueIndex = clientAddr.hashCode() % queues.size();
                queues.get(queueIndex).put(key);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (channel != null) {
                channel.close();
            }
        }
    }


    /**
     * 真正的工作线程
     */
    class Worker extends Thread {
        private final LinkedBlockingQueue<SelectionKey> queue;

        public Worker(LinkedBlockingQueue<SelectionKey> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (true) {
                SocketChannel channel = null;
                try {
                    //从queue获取SelectionKey，并开始处理请求
                    SelectionKey key = queue.take();
                    channel = (SocketChannel) key.channel();
                    handleRequest(channel, key);
                } catch (Exception e) {
                    e.printStackTrace();
                    if (channel != null) {
                        try {
                            channel.close();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }

        }

        /**
         * 处理客户端发送过来的请求
         *
         * @param channel 客户端channel
         * @param key     路由key
         * @throws IOException
         */
        private void handleRequest(SocketChannel channel, SelectionKey key) throws Exception {
            if (!channel.isOpen()) {
                return;
            }
            //客户端的ip地址
            String clientAddr = channel.getRemoteAddress().toString();
            System.out.println("接收到客户端的请求：" + clientAddr);

            // 需要先提取出来这次请求是什么类型：1 发送文件；2 读取文件
            if (cacheRequests.containsKey(clientAddr)) {
                handleSendFileRequest(channel, key);
                return;
            }
            Integer requestType = getRequestType(channel);
            if (requestType == null) {
                return;
            }
            System.out.println("从请求中解析出来请求类型：" + requestType);

            // 拆包，就是说人家一次请求，本来是包含了：requestType + filenameLength + filename [+ imageLength + image]
            // 这次OP_READ事件，就读取到了requestType的4个字节中的2个字节，剩余的数据
            // 就被放在了下一次OP_READ事件中了
            if (SEND_FILE.equals(requestType)) {
                handleSendFileRequest(channel, key);
            } else if (READ_FILE.equals(requestType)) {
                handleReadFileRequest(channel, key);
            }
        }

        /**
         * 获取本次请求的类型
         *
         * @param channel 客户端的channel
         * @return 请求的类型
         */
        private Integer getRequestType(SocketChannel channel) throws IOException {
            String clientAddr = channel.getRemoteAddress().toString();
            if (getCachedRequest(clientAddr).getRequestType() != null) {
                return cacheRequests.get(clientAddr).getRequestType();
            }
            Integer requestType = null;
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
                cachedRequest.setRequestType(requestType);
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


        private void handleSendFileRequest(SocketChannel channel, SelectionKey key) throws IOException {
            String clientAddr = channel.getRemoteAddress().toString();
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

            //定义已经读取文件的大小
            Long hasReadImageLength = getHasReadImageLength(channel);
            System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);

            //构建本地文件输出流
            FileOutputStream fos = null;
            FileChannel fileChannel = null;
            try {
                fos = new FileOutputStream(filename.getAbsoluteFilenamePath());
                fileChannel = fos.getChannel();
                fileChannel.position(fileChannel.size());

                //循环不断的从channel里读取数据，并写入磁盘文件
                ByteBuffer fileBuffer;
                if (fileByClient.containsKey(clientAddr)) {
                    fileBuffer = fileByClient.get(clientAddr);
                } else {
                    fileBuffer = ByteBuffer.allocate(Math.toIntExact(fileLength));
                }
                hasReadImageLength += channel.read(fileBuffer);

                if (!fileBuffer.hasRemaining()) {
                    fileBuffer.rewind();
                    int written = fileChannel.write(fileBuffer);
                    fileByClient.remove(clientAddr);
                    System.out.println("本次文件上传完毕，将" + written + " bytes的数据写入本地磁盘文件.......");

                    ByteBuffer data = ByteBuffer.wrap("SUCCESS".getBytes());
                    channel.write(data);
                    cacheRequests.remove(clientAddr);
                    System.out.println("文件读取完毕，返回响应给客户端。。。。");

                    //增量上报Master节点自己已经接收到一个副本
                    nameNodeRpcClient.informReplicaReceived(filename.getRelativeFilenamePath());
                    System.out.println("增量上报收到的文件副本给NameNode节点,path=【"+filename.getRelativeFilenamePath()+"】");

                    key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                } else {
                    fileByClient.put(clientAddr, fileBuffer);
                    CachedRequest cachedRequest = getCachedRequest(clientAddr);
                    cachedRequest.setHasReadFileLength(hasReadImageLength);
                    System.out.println("本次文件上传出现拆包问题，缓存起来，下次继续读取.......");
                }
            } finally {
                if (fos != null) {
                    fos.close();
                }
                if (fileChannel != null) {
                    fileChannel.close();
                }
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

            File file = new File(filenamePath.getAbsoluteFilenamePath());
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
            if (cacheRequests.get(clientAddr).getFilenamePath() != null) {
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
                cachedRequest.setFilenamePath(filename);
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
            StringBuilder dirPath = new StringBuilder(dataDir);
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
            if (cacheRequests.get(clientAddr).getFileLength() != null) {
                fileLength = cacheRequests.get(clientAddr).getFileLength();
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
         * 获取已读文件大小长度
         *
         * @param channel 客户端channel
         * @return 已读文件大小长度
         */
        private Long getHasReadImageLength(SocketChannel channel) throws IOException {
            String clientAddr = channel.getRemoteAddress().toString();
            if (cacheRequests.get(clientAddr).getHasReadFileLength() != null) {
                return cacheRequests.get(clientAddr).getHasReadFileLength();
            }
            return 0L;
        }

    }

}
