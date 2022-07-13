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
 * Project:  hdfs-study
 * CreateDate: Created in 2022-07-13 10:10
 *
 * @author linhaibo
 */
public class FSImageUploadServer extends Thread {

    private Selector selector;

    public FSImageUploadServer() {
        init();
    }

    /**
     * 上传文件server的初始化
     */
    public void init() {
        try {
            ServerSocketChannel serverSocketChannel = null;
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(9000), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.out.println("FSImageUploadServer启动，监听9000端口......");
        while (true) {
            try {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();
                    try {
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
            handleReadRequest(selectionKey);
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
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
            socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                System.out.println("已经和客户端建立好连接.....");
                socketChannel.configureBlocking(false);
                socketChannel.register(selector, SelectionKey.OP_READ);
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
    private void handleReadRequest(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = null;

        try {
            //把上一次的文件删除了
            String fsimageFilePath = "/Users/linhaibo/Documents/tmp/namenode/fsimage.meta";
            File fsimageFile = new File(fsimageFilePath);
            if (fsimageFile.exists()) {
                fsimageFile.delete();
            }
            RandomAccessFile fsimageFileRAF = null;
            FileOutputStream fsimageOut = null;
            FileChannel fsimageFileChannel = null;

            try {
                fsimageFileRAF = new RandomAccessFile(fsimageFilePath, "rw");
                fsimageOut = new FileOutputStream(fsimageFileRAF.getFD());
                fsimageFileChannel = fsimageOut.getChannel();

                socketChannel = (SocketChannel) selectionKey.channel();
                ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

                while (socketChannel.read(buffer) > 0) {
                    System.out.println("接收fsimage文件以及写入本地磁盘完毕......");
                    buffer.flip();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();
                }
                fsimageFileChannel.force(false);
            } finally {
                if (fsimageFileRAF != null) {
                    fsimageFileRAF.close();
                }
                if (fsimageOut != null) {
                    fsimageOut.close();
                }
                if (fsimageFileChannel != null) {
                    fsimageFileChannel.close();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            if (socketChannel != null) {
                socketChannel.close();
            }
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
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            buffer.put("SUCCESS".getBytes());
            buffer.flip();

            socketChannel = (SocketChannel) selectionKey.channel();
            socketChannel.write(buffer);

            System.out.println("fsimage上传完毕，返回响应SUCCESS给backupnode......");

            socketChannel.register(selector, SelectionKey.OP_READ);
        } catch (IOException e) {
            e.printStackTrace();
            if (socketChannel != null) {
                socketChannel.close();
            }
        }
    }


}
