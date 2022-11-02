package com.hepo.dfs.namenode.server;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * Description: 负责fsImage文件上传的server
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-07-13 10:10
 *
 * @author linhaibo
 */
public class FSImageUploadServer extends Thread {
    /**
     * IO多路复用
     */
    private Selector selector;


    /**
     * FsImagesUpload组件的默认端口
     */
    private final static int DEFAULT_PORT = 9000;

    /**
     * FsImagesUpload组件的默认最大连接数
     */
    private final static int DEFAULT_BACKLOG = 100;

    /**
     * 默认缓存区大小：1MB
     */
    private final static int DEFAULT_BUFFER_SIZE = 1024 * 1024;

    private volatile boolean isRunning = true;

    public FSImageUploadServer() {
        init();
    }

    /**
     * 上传文件server的初始化
     */
    public void init() {
        ServerSocketChannel serverSocketChannel = null;
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(DEFAULT_PORT), DEFAULT_BACKLOG);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                if (serverSocketChannel != null){
                    serverSocketChannel.close();
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void run() {
        System.out.println("FSImageUploadServer启动，监听9000端口......");
        while (isRunning) {
            try {
                //轮询selector
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    //获取selectionKey
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();
                    try {
                        //处理请求
                        handleRequest(selectionKey);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 处理客户端发送过来的请求
     *
     * @param selectionKey A token representing the registration of a SelectableChannel with a Selector
     */
    private void handleRequest(SelectionKey selectionKey) throws IOException {
        if (selectionKey.isAcceptable()) {
            handleConnectRequest(selectionKey);
        } else if (selectionKey.isReadable()) {
            handleReadableRequest(selectionKey);
        } else if (selectionKey.isWritable()) {
            handleWriteRequest(selectionKey);
        }
    }

    /**
     * 处理BackupNode连接请求
     *
     * @param selectionKey A token representing the registration of a SelectableChannel with a Selector
     */
    private void handleConnectRequest(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = null;
        try {
            //获取客户端的channel，将该channel注册到selector的读事件中
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
            socketChannel = serverSocketChannel.accept();

            if (socketChannel != null) {
                socketChannel.configureBlocking(false);
                //将该客户端连接channel注册到selector的Read事件
                socketChannel.register(selector, SelectionKey.OP_READ);
                selectionKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        } catch (IOException e) {
            e.printStackTrace();
            if (socketChannel != null) {
                socketChannel.close();
            }
        }
    }

    /**
     * 处理发送fsimage文件的请求
     *
     * @param selectionKey A token representing the registration of a SelectableChannel with a Selector
     */
    private void handleReadableRequest(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = null;

        try {
            String fsimageFilePath = "/Users/linhaibo/Documents/tmp/editslog/fsimage.meta";

            RandomAccessFile raf = null;
            FileOutputStream fos = null;
            FileChannel fileChannel = null;

            try {
                //获取channel
                socketChannel = (SocketChannel) selectionKey.channel();
                //分配一兆的缓冲区
                ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
                int total = 0;
                int count;
                if ((count = socketChannel.read(buffer)) > 0) {
                    File fsimageFile = new File(fsimageFilePath);
                    if (fsimageFile.exists()) {
                        fsimageFile.delete();
                    }
                    raf = new RandomAccessFile(fsimageFilePath, "rw");
                    fos = new FileOutputStream(raf.getFD());
                    fileChannel = fos.getChannel();
                    total += count;

                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.clear();
                }else {
                    //如果没有读到数据,继续监听
                    socketChannel.register(selector, SelectionKey.OP_READ);
                    return;
                }

                while ((count = socketChannel.read(buffer)) > 0) {
                    //统计读到的数据大小
                    total += count;
                    //写buffer之前执行flip操作
                    buffer.flip();
                    //往fileChannel写文件
                    fileChannel.write(buffer);
                    //写完数据记得清除缓冲区，避免内存溢出
                    buffer.clear();
                }

                if (total > 0) {
                    System.out.println("接收fsimage文件以及写入本地磁盘完毕......");
                    fileChannel.force(false);
                    socketChannel.register(selector, SelectionKey.OP_WRITE);
                }


            } finally {
                if (raf != null) {
                    raf.close();
                }
                if (fos != null) {
                    fos.close();
                }
                if (fileChannel != null) {
                    fileChannel.close();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            socketChannel.close();
        }
    }

    /**
     * 处理返回响应给BackupNode
     *
     * @param selectionKey A token representing the registration of a SelectableChannel with a Selector
     */
    private void handleWriteRequest(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = null;

        try {
            ByteBuffer buffer = ByteBuffer.allocate( 1024);
            buffer.put("SUCCESS!".getBytes());
            buffer.flip();

            socketChannel = (SocketChannel) selectionKey.channel();
            socketChannel.write(buffer);
            buffer.clear();

            System.out.println("fsimage上传完毕，返回响应SUCCESS给backupnode......");
            //又重新注册Read事件，下次客户端发送请求又可以监听，这就是IO多路复用的效果
            socketChannel.register(selector, SelectionKey.OP_READ);
        } catch (IOException e) {
            e.printStackTrace();
            //如果发生异常，则关闭该客户端的channel连接
            socketChannel.close();
        }
    }


}
