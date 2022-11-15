package com.hepo.dfs.client.datanode.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.hepo.dfs.client.datanode.server.DataNodeConfig.FILE_UPLOAD_SERVER_PORT;

/**
 * Description:文件上传服务端
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/4 10:57
 *
 * @author linhaibo
 */
public class FileUploadServer extends Thread {

    /**
     * Processor线程数
     */
    public static final Integer PROCESSOR_THREAD_NUM = 10;

    /**
     * IO Thread线程数
     */
    public static final Integer IO_THREAD_NUM = 10;
    /**
     * IO多路复用用器，负责监听多个连接请求
     */
    private Selector selector;

    /**
     * 与NameNode通信的Rpc客户端
     */
    private NameNodeRpcClient namenode;
    /**
     * 存放processor线程的队列
     */
    private final List<FileUploadProcessor> processors = new ArrayList<>(PROCESSOR_THREAD_NUM);

    public FileUploadServer(NameNodeRpcClient namenode) {
        this.namenode =namenode;
    }


    /**
     * NIOServer的初始化，监听端口、队列初始化、线程初始化
     */
    public void init() {
        ServerSocketChannel ssc;
        try {
            selector = Selector.open();
            ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            ssc.socket().bind(new InetSocketAddress(FILE_UPLOAD_SERVER_PORT), 100);
            ssc.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("FileUploadServer已经启动，监听端口号：" + FILE_UPLOAD_SERVER_PORT);

            NetworkResponseQueue responseQueue = NetworkResponseQueue.getInstance();
            //启动固定数量的Processor线程
            for (int i = 0; i < PROCESSOR_THREAD_NUM; i++) {
                FileUploadProcessor processor = new FileUploadProcessor(i);
                processors.add(processor);
                processor.start();
                responseQueue.initReponseQueue(i);
            }
            //启用固定数量的IO线程
            for (int i = 0; i < IO_THREAD_NUM; i++) {
                IOThread ioThread = new IOThread(namenode);
                ioThread.start();
            }
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
                    if (key.isAcceptable()) {
                        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                        //跟每个客户端建立连接
                        SocketChannel channel = serverSocketChannel.accept();
                        if (channel != null) {
                            channel.configureBlocking(false);
                            // 如果一旦跟某个客户端建立了连接之后，就需要将这个客户端均匀的分发给后续的Processor线程
                            int processorIndex = new Random().nextInt(PROCESSOR_THREAD_NUM);
                            FileUploadProcessor processor = processors.get(processorIndex);
                            processor.addChannel(channel);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
