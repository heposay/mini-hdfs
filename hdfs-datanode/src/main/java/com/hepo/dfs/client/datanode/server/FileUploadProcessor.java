package com.hepo.dfs.client.datanode.server;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Description:负责解析请求以及发送响应的线程
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 00:23
 *
 * @author linhaibo
 */
public class FileUploadProcessor extends Thread {

    /**
     * 多路复用监听时的最大阻塞时间
     */
    public static final Long POLL_BLOCK_MAX_TIME = 1000L;

    /**
     * 等待注册的网络连接的队列
     */
    private final ConcurrentLinkedQueue<SocketChannel> queue = new ConcurrentLinkedQueue<>();

    /**
     * 缓存没读取完的请求
     */
    private final Map<String, NetworkRequest> cachedRequest = new HashMap<>();

    /**
     * 缓存响应结果
     */
    private final Map<String, NetworkResponse> cachedResponse = new HashMap<>();
    /**
     * 这个processor负责维护的所有客户端的SelectionKey
     */
    private Map<String, SelectionKey> cachedKeys = new HashMap<>();
    /**
     * processor标识
     */
    private Integer processorId;

    /**
     * 每个Processor私有的Selector多路复用器
     */
    private Selector selector;

    public FileUploadProcessor(Integer processorId) {
        this.processorId = processorId;
        try {
            this.selector = Selector.open();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public Integer getProcessorId() {
        return processorId;
    }

    public void setProcessorId(Integer processorId) {
        this.processorId = processorId;
    }

    /**
     * 给这个Processor线程分配一个网络连接
     *
     * @param channel 客户端连接
     */
    public void addChannel(SocketChannel channel) {
        queue.offer(channel);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (true) {
            try {
                //注册排队等待的连接
                registerQueueClients();
                //处理排队中的响应
                cacheQueueResponse();
                //以限时阻塞的方式感知连接中的请求
                poll();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 以多路复用的方式来监听各个连接的请求
     */
    private void poll() {
        try {
            int keys = selector.select(POLL_BLOCK_MAX_TIME);
            if (keys > 0) {
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    SocketChannel channel = (SocketChannel) key.channel();
                    String clientAddr = channel.getRemoteAddress().toString();

                    if (key.isReadable()) {

                        NetworkRequest networkRequest = cachedRequest.get(clientAddr);
                        if (networkRequest == null) {
                            networkRequest = new NetworkRequest(key, channel);
                        }
                        //开始处理请求
                        networkRequest.read();
                        if (networkRequest.hasCompletedRead()) {
                            networkRequest.setProcessorId(processorId);
                            networkRequest.setClientAddr(clientAddr);
                            // 此时就可以将一个请求分发到全局的请求队列里去了
                            NetworkRequestQueue.getInstance().offer(networkRequest);

                            //处理完毕之后，移除缓存
                            cachedRequest.remove(clientAddr);
                            //取消订阅读事件
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                        }else {
                            //如果请求还没处理完毕，直接缓存起来。等下次再处理
                            cachedRequest.put(clientAddr, networkRequest);
                        }
                    }else if (key.isWritable()) {
                        //返回响应结果给客户端
                        NetworkResponse response = cachedResponse.get(clientAddr);
                        channel.write(response.getBuffer());

                        //处理完毕之后，移除缓存
                        cachedResponse.remove(clientAddr);
                        cachedKeys.remove(clientAddr);
                        //重新注册读事件
                        key.interestOps(SelectionKey.OP_READ);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 将排队中的等待注册的连接注册到Selector上去
     */
    private void registerQueueClients() throws IOException {
        SocketChannel channel = null;
        while ((channel = queue.poll()) != null) {
            try {
                channel.register(selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                channel.close();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 缓存响应结果
     */
    private void cacheQueueResponse() {
        NetworkResponseQueue responseQueue = NetworkResponseQueue.getInstance();
        NetworkResponse response = null;
        while ((response = responseQueue.poll(processorId)) != null) {
            String clientAddr = response.getClientAddr();
            cachedResponse.put(clientAddr, response);
            cachedKeys.get(clientAddr).interestOps(SelectionKey.OP_WRITE);
        }
    }
}
