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
     * 图片缓存，用于解决拆包问题
     */
    private final Map<String, CacheImage> cacheImageMap = new ConcurrentHashMap<>();

    /**
     * 与namenode进行通信的客户端
     */
    private NameNodeRpcClient nameNodeRpcClient;


    private String dataDir = DATA_DIR;

    static class CacheImage {
        FilenamePath filenamePath;
        long imageLength;
        long hasReadImageLength;

        public CacheImage(FilenamePath filenamePath, long imageLength, long hasReadImageLength) {
            this.filenamePath = filenamePath;
            this.imageLength = imageLength;
            this.hasReadImageLength = hasReadImageLength;
        }

        @Override
        public String toString() {
            return "CacheImage{" +
                    "filenamePath='" + filenamePath + '\'' +
                    ", imageLength=" + imageLength +
                    ", hasReadImageLength=" + hasReadImageLength +
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
            return "FilenamePath{" +
                    "relativeFilenamePath='" + relativeFilenamePath + '\'' +
                    ", absoluteFilenamePath='" + absoluteFilenamePath + '\'' +
                    '}';
        }
    }

    /**
     * NIOServer的初始化，监听端口、队列初始化、线程初始化
     *
     * @param nameNodeRpcClient namenode的客户端
     */
    public FileUploadServer(NameNodeRpcClient nameNodeRpcClient) {
        ServerSocketChannel ssc = null;
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
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            if (ssc != null) {
                try {
                    ssc.close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
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
                            throw new RuntimeException(ex);
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
        private void handleRequest(SocketChannel channel, SelectionKey key) throws IOException {
            if (!channel.isOpen()) {
                channel.close();
                return;
            }

            //客户端的ip地址
            String clientAddr = channel.getRemoteAddress().toString();
            System.out.println("接收到客户端的请求：" + clientAddr);

            if (cacheImageMap.containsKey(clientAddr)) {
                handleSendFileRequest(channel, key);
            } else {
                Integer requestType = getRequestType(channel);
                if (SEND_FILE.equals(requestType)) {
                    handleSendFileRequest(channel, key);
                } else if (READ_FILE.equals(requestType)) {
                    handleReadFileRequest(channel, key);
                }
            }
        }

        /**
         * 获取本次请求的类型
         *
         * @param channel 客户端的channel
         * @return 请求的类型
         */
        private Integer getRequestType(SocketChannel channel) throws IOException{
            ByteBuffer requestTypeBuffer = ByteBuffer.allocate(4);
            channel.read(requestTypeBuffer);
            if (!requestTypeBuffer.hasRemaining()) {
                requestTypeBuffer.rewind();
                return requestTypeBuffer.getInt();
            }
            return -1;
        }


        private void handleSendFileRequest(SocketChannel channel, SelectionKey key) throws IOException {
            String clientAddr = channel.getRemoteAddress().toString();
            //从请求中解析文件名
            FilenamePath filename = getFileName(channel);
            System.out.println("从网络请求解析出来文件名：" + filename);
            if (filename == null) {
                channel.close();
                return;
            }

            //从请求中解析文件大小
            long imageLength = getImageLength(channel);
            System.out.println("从网络请求解析出来文件的文件大小：" + imageLength);

            //定义已经读取文件的大小
            long hasReadImageLength = getHasReadImageLength(channel);
            System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);

            //构建本地文件输出流
            FileOutputStream fos = new FileOutputStream(filename.getAbsoluteFilenamePath());
            FileChannel fileChannel = fos.getChannel();
            fileChannel.position(fileChannel.size());

            //循环不断的从channel里读取数据，并写入磁盘文件
            ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
            int len = -1;
            while ((len = channel.read(buffer)) > 0) {
                hasReadImageLength += len;
                System.out.println("已经向本地磁盘写入了" + hasReadImageLength + "字节的数据了");
                buffer.flip();
                fileChannel.write(buffer);
                buffer.clear();
            }

            fos.close();
            fileChannel.close();

            //判断一下，如果已经读取完毕，返回一个成功给客户端
            if (hasReadImageLength == imageLength) {
                ByteBuffer data = ByteBuffer.wrap("SUCCESS".getBytes());
                channel.write(data);
                cacheImageMap.remove(clientAddr);
                System.out.println("文件读取完毕，返回响应给客户端。。。。");

                //增量上报Master节点自己已经接收到一个副本
                nameNodeRpcClient.informReplicaReceived(filename.getAbsoluteFilenamePath());
                System.out.println("增量上报收到的文件副本给NameNode节点......");
            } else {
                CacheImage cacheImage = new CacheImage(filename, imageLength, hasReadImageLength);
                cacheImageMap.put(clientAddr, cacheImage);
                System.out.println("文件没有读取完毕，等待下一次OP_READ请求，缓存文件" + cacheImage);
            }
        }

        /**
         * 读取DataNode磁盘上的文件
         * @param channel
         * @param key
         */
        private void handleReadFileRequest(SocketChannel channel, SelectionKey key) throws IOException{
            String clientAddr = channel.getRemoteAddress().toString();

            //从请求中解析出文件的路径
            FilenamePath filenamePath = getFileName(channel);
            System.out.println("从客户端请求中解析出文件目录名：" + filenamePath);
            if (filenamePath == null) {
                channel.close();
                return;
            }

            File file = new File(filenamePath.getAbsoluteFilenamePath());
            long fileLength = file.length();

            FileInputStream fis = new FileInputStream(file);
            FileChannel fileChannel = fis.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate((int) (fileLength * 2));
            long hasReadFileLength = 0L;
            int len = -1;

            while ((len = fileChannel.read(buffer)) > 0) {
                hasReadFileLength += len;
                System.out.println("已经在磁盘中读取了" + hasReadFileLength + "字节的数据...");
                buffer.flip();
                //将读到的文件数据发送给客户端
                channel.write(buffer);
                buffer.clear();
            }
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
            if (cacheImageMap.containsKey(clientAddr)) {
                filename = cacheImageMap.get(clientAddr).filenamePath;
            } else {
                //获取文件名
                String relativeFilenamePath = getRelativeFilenamePath(channel);
                if (relativeFilenamePath == null) {
                    return null;
                }
                filename.relativeFilenamePath = relativeFilenamePath;
                filename.absoluteFilenamePath = getAbsoluteFilenamePath(relativeFilenamePath);
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
            String filename = null;
            ByteBuffer filenameLengthBuffer = ByteBuffer.allocate(4);
            channel.read(filenameLengthBuffer);
            if (!filenameLengthBuffer.hasRemaining()) {
                filenameLengthBuffer.rewind();
                //先读取4个字节的文件名长度
                int filenameLength = filenameLengthBuffer.getInt();

                //分配新的缓冲区，将要读取的数据放进去，然后在读取出来
                ByteBuffer filenameBuffer = ByteBuffer.allocate(filenameLength);
                channel.read(filenameBuffer);
                if (!filenameBuffer.hasRemaining()) {
                    filenameBuffer.rewind();
                    filename = new String(filenameBuffer.array());
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
        private long getImageLength(SocketChannel channel) throws IOException {
            long imageLength = 0L;
            String clientAddr = channel.getRemoteAddress().toString();
            if (cacheImageMap.containsKey(clientAddr)) {
                imageLength = cacheImageMap.get(clientAddr).imageLength;
            } else {
                ByteBuffer imageLengthBuffer = ByteBuffer.allocate(4);
                channel.read(imageLengthBuffer);
                if (!imageLengthBuffer.hasRemaining()) {
                    imageLengthBuffer.rewind();
                    imageLength = imageLengthBuffer.getLong();
                }
            }
            return imageLength;
        }

        /**
         * 获取已读文件大小长度
         *
         * @param channel 客户端channel
         * @return 已读文件大小长度
         */
        private long getHasReadImageLength(SocketChannel channel) throws IOException {
            long hasReadImageLength = 0L;
            String clientAddr = channel.getRemoteAddress().toString();
            if (cacheImageMap.containsKey(clientAddr)) {
                hasReadImageLength = cacheImageMap.get(clientAddr).hasReadImageLength;
            }
            return hasReadImageLength;
        }

    }

}
