package com.hepo.dfs.client.datanode.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
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

    /**
     * IO多路复用用器
     */
    private Selector selector;
    /**
     * 存放处理请求的队列
     */
    private final List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();
    /**
     * 图片缓存，用于解决拆包问题
     */
    private final Map<String, CacheImage> cacheImageMap = new HashMap<>();

    static class CacheImage {
        String fileName;
        long imageLength;
        long hasReadImageLength;

        public CacheImage(String fileName, long imageLength, long hasReadImageLength) {
            this.fileName = fileName;
            this.imageLength = imageLength;
            this.hasReadImageLength = hasReadImageLength;
        }

        @Override
        public String toString() {
            return "CacheImage{" +
                    "fileName='" + fileName + '\'' +
                    ", imageLength=" + imageLength +
                    ", hasReadImageLength=" + hasReadImageLength +
                    '}';
        }
    }

    public FileUploadServer() {
        ServerSocketChannel ssc = null;
        try {
            selector = Selector.open();

            ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            ssc.socket().bind(new InetSocketAddress(FILE_UPLOAD_SERVER_PORT));
            ssc.register(selector, SelectionKey.OP_ACCEPT);

            for (int i = 0; i < 3; i++) {
                queues.add(new LinkedBlockingQueue<>());
            }
            for (int i = 0; i < 3; i++) {
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
                    handleRequest(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 处理客户端发送过来的请求
     * 主线程负责将请求放到对应的queue，然后worker线程负责从queue获取请求，并处理
     * 所以这里性能非常高
     */
    private void handleRequest(SelectionKey key) {
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
            throw new RuntimeException(e);
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
                    if (!channel.isOpen()) {
                        channel.close();
                        return;
                    }

                    //客户端的ip地址
                    String clientAddr = channel.getRemoteAddress().toString();
                    System.out.println("接收到客户端的请求：" + clientAddr);
                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);

                    //从请求中解析文件名
                    String filename = getFileName(channel, buffer);
                    System.out.println("从网络请求解析出来文件名：" + filename);
                    if (filename == null) {
                        channel.close();
                        return;
                    }

                    //从请求中解析文件大小
                    long imageLength = getImageLength(channel, buffer);

                    System.out.println("从网络请求解析出来文件的文件大小：" + imageLength);

                    //定义已经读取文件的大小
                    long hasReadImageLength = getHasReadImageLength(channel);

                    System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);

                    //构建本地文件输出流
                    FileOutputStream fos = new FileOutputStream(filename);
                    FileChannel fileChannel = fos.getChannel();
                    fileChannel.position(fileChannel.size());

                    //如果第一次收到请求，应该把buffer剩余的数据写到文件里去
                    if (!cacheImageMap.containsKey(clientAddr)) {
                        hasReadImageLength += fileChannel.write(buffer);
                        buffer.clear();
                    }

                    //不断从channel读取数据，并写入磁盘文件
                    int len = -1;
                    while ((len = channel.read(buffer)) > 0) {
                        hasReadImageLength += len;
                        System.out.println("已经向本地磁盘写入了" + hasReadImageLength + "字节的数据了");
                        buffer.flip();
                        fileChannel.write(buffer);
                        buffer.clear();
                    }

                    //判断一下，如果已经读取完毕，返回一个成功给客户端
                    if (hasReadImageLength == imageLength) {
                        ByteBuffer data = ByteBuffer.wrap("SUCCESS".getBytes());
                        channel.write(data);
                        cacheImageMap.remove(clientAddr);
                        System.out.println("文件读取完毕，返回响应给客户端。。。。");
                    } else {
                        CacheImage cacheImage = new CacheImage(filename, imageLength, hasReadImageLength);
                        cacheImageMap.put(clientAddr, cacheImage);
                        key.interestOps(SelectionKey.OP_READ);
                        System.out.println("文件没有读取完毕，等待下一次OP_READ请求，缓存文件" + cacheImage);
                    }

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
         * 获取filename并创建文件
         *
         * @param channel 客户端channel
         * @param buffer  缓冲区
         * @return 文件名
         * @throws IOException 如果客户端连接发生断开，会抛出该异常。
         */
        private String getFileName(SocketChannel channel, ByteBuffer buffer) throws IOException {
            String filename = null;
            String clientAddr = channel.getRemoteAddress().toString();
            //尝试从缓存中获取，如果没有，则从channel里面获取
            if (cacheImageMap.containsKey(clientAddr)) {
                filename = cacheImageMap.get(clientAddr).fileName;
            } else {
                //获取文件名
                filename = getFileNameFromChannel(channel, buffer);
                if (filename == null) {
                    return null;
                }
                //解析文件名，然后创建目录
                String[] splitDir = filename.split("/");
                StringBuilder dirPath = new StringBuilder(DATA_DIR);
                for (int i = 1; i < splitDir.length - 1; i++) {
                    dirPath.append("/").append(splitDir[i]);
                }

                File dir = new File(dirPath.toString());
                if (!dir.exists()) {
                    dir.mkdirs();
                }
                //拼接最终的filename
                filename = dirPath + "/" + splitDir[splitDir.length - 1];
            }
            return filename;
        }

        /**
         * 从网络请求获取文件名
         *
         * @param channel 客户端channel
         * @param buffer  数据缓冲区
         * @return 文件名
         */
        private String getFileNameFromChannel(SocketChannel channel, ByteBuffer buffer) throws IOException {
            int len = channel.read(buffer);
            if (len > 0) {
                buffer.flip();
                //先读取4个字节的文件名长度
                byte[] fileNameLengthBytes = new byte[4];
                buffer.get(fileNameLengthBytes, 0, 4);

                //分配新的缓冲区，将要读取的数据放进去，然后在读取出来
                ByteBuffer fileNameLengthBuffer = ByteBuffer.wrap(fileNameLengthBytes);
                fileNameLengthBuffer.put(fileNameLengthBytes);
                fileNameLengthBuffer.flip();
                int fileNameLength = fileNameLengthBuffer.getInt();

                //再读取第4个字节到文件长度的内容，将最终结果转译成文件名
                byte[] fileNameBytes = new byte[fileNameLength];
                buffer.get(fileNameBytes, 0, fileNameLength);
                return new String(fileNameBytes);
            }
            return null;
        }

        /**
         * 获取文件大小长度
         *
         * @param channel 客户端channel
         * @param buffer  缓冲区
         * @return 文件长度
         */
        private long getImageLength(SocketChannel channel, ByteBuffer buffer) throws IOException {
            long imageLength = 0L;
            String clientAddr = channel.getRemoteAddress().toString();
            if (cacheImageMap.containsKey(clientAddr)) {
                imageLength = cacheImageMap.get(clientAddr).imageLength;
            }else {
                byte[] imageLengthBytes = new byte[8];
                buffer.get(imageLengthBytes, 0, 8);

                ByteBuffer imageLengthBuffer = ByteBuffer.wrap(imageLengthBytes);
                imageLengthBuffer.put(imageLengthBytes);
                imageLengthBuffer.flip();
                imageLength = imageLengthBuffer.getLong();
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
