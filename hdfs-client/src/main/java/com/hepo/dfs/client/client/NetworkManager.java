package com.hepo.dfs.client.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Description:网络连接管理组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 18:29
 *
 * @author linhaibo
 */
public class NetworkManager {

    /**
     * 正在连接中
     */
    public static final Integer CONNECTING = 1;
    /**
     * 已经连接
     */
    public static final Integer CONNECTED = 2;
    /**
     * 断开连接
     */
    public static final Integer DISCONNECTED = 3;
    /**
     * 网络poll操作的超时时间
     */
    private static final Long POLL_TIMEOUT = 500L;
    /**
     * 请求超时检测间隔
     */
    private static final long REQUEST_TIMEOUT_CHECK_INTERVAL = 1000;
    /**
     * 请求超时时长
     */
    private static final long REQUEST_TIMEOUT = 30 * 1000;
    /**
     * 多路复用器
     */
    private Selector selector;
    /**
     * 每个数据节点的连接状态
     */
    private Map<String, Integer> connectState;
    /**
     * 存放已经建立好的连接
     */
    private Map<String, SelectionKey> connects;
    /**
     * 等待建立连接的机器
     */
    private ConcurrentLinkedQueue<Host> waitingConnectHosts;
    /**
     * 等待发送网络请求的队列
     */
    private Map<String, ConcurrentLinkedQueue<NetworkRequest>> waitingRequests;
    /**
     * 马上准备要发送的网络请求队列
     */
    private Map<String, NetworkRequest> toSendRequests;
    /**
     * 已完成的请求响应的队列
     */
    private Map<String, NetworkResponse> finishedResponses;


    public NetworkManager() {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.connectState = new ConcurrentHashMap<>();
        this.connects = new ConcurrentHashMap<>();
        this.waitingConnectHosts = new ConcurrentLinkedQueue<>();
        this.waitingRequests = new ConcurrentHashMap<>();
        this.toSendRequests = new ConcurrentHashMap<>();
        this.finishedResponses = new ConcurrentHashMap<>();

        //启动处理网络连接的核心线程
        new NetworkPollThread().start();
        //启动请求超时检测线程
        new RequestTimeoutCheckThread().start();
    }

    /**
     * 尝试建立连接
     *
     * @param hostname         DataNode的主机名
     * @param uploadServerPort DataNode的上传端口
     */
    public Boolean maybeConnect(String hostname, Integer uploadServerPort) {
        synchronized (this) {
            if (!connectState.containsKey(hostname) || connectState.get(hostname).equals(DISCONNECTED)) {
                connectState.put(hostname, CONNECTING);
                waitingConnectHosts.offer(new Host(hostname, uploadServerPort));
            }

            while (CONNECTING.equals(connectState.get(hostname))) {
                //如果连接还没建立完成，释放锁等待100ms
                try {
                    wait(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (DISCONNECTED.equals(connectState.get(hostname))) {
                return false;
            }
            return true;
        }
    }

    /**
     * 发送网络请求
     *
     * @param request 网络请求
     */
    public void sendRequest(NetworkRequest request) {
        ConcurrentLinkedQueue<NetworkRequest> requestQueue = waitingRequests.get(request.getHostname());
        requestQueue.offer(request);
    }

    /**
     * 等待指定请求的响应
     *
     * @param requestId 网络请求的ID
     * @return 是否请求成功
     */
    public NetworkResponse waitResponse(String requestId) throws InterruptedException {
        NetworkResponse response = null;
        while ((response = finishedResponses.get(requestId)) == null) {
            Thread.sleep(100);
        }
        toSendRequests.remove(response.getHostname());
        finishedResponses.remove(requestId);

        return response;
    }

    /**
     * 处理网络连接的核心线程
     */
    class NetworkPollThread extends Thread {
        @Override
        public void run() {
            tryConnect();
            prepareRequests();
            poll();
        }

        /**
         * 尝试把排队中与DataNode建立连接
         */
        private void tryConnect() {
            Host host = null;
            SocketChannel channel = null;
            try {
                while ((host = waitingConnectHosts.poll()) != null) {
                    channel = SocketChannel.open();
                    channel.configureBlocking(false);
                    channel.connect(new InetSocketAddress(host.getHostname(), host.getUploadServerPort()));
                    channel.register(selector, SelectionKey.OP_CONNECT);
                }
            } catch (IOException e) {
                e.printStackTrace();
                connectState.put(host.getHostname(), DISCONNECTED);
            }
        }

        /**
         * 准备好要发送的请求
         */
        private void prepareRequests() {
            for (String hostname : waitingRequests.keySet()) {
                ConcurrentLinkedQueue<NetworkRequest> requestQueue = waitingRequests.get(hostname);
                //判断这台机器当前是否还没有请求马上就要发送出去
                if (!requestQueue.isEmpty() && !toSendRequests.containsKey(hostname)) {
                    //将请求放到toSendRequests的队列中
                    NetworkRequest request = requestQueue.poll();
                    toSendRequests.put(hostname, request);

                    //让这台机器连接关注事件为OP_WRITE
                    SelectionKey key = connects.get(hostname);
                    key.interestOps(SelectionKey.OP_WRITE);
                }
            }
        }

        /**
         * 尝试完成网络连接、请求发送、响应读取
         */
        private void poll() {
            SocketChannel channel = null;
            try {
                int selectKeys = selector.select(POLL_TIMEOUT);
                if (selectKeys <= 0) {
                    return;
                }
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();
                    channel = (SocketChannel) key.channel();
                    if (key.isConnectable()) {
                        finishedConnect(key, channel);
                    } else if (key.isWritable()) {
                        sendRequest(key, channel);
                    } else if (key.isReadable()) {
                        readResponse(key, channel);
                    }
                }
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


        /**
         * 完成与DataNode数据节点连接
         *
         * @param key     关注事件的key
         * @param channel 客户端的channel
         */
        private void finishedConnect(SelectionKey key, SocketChannel channel) {
            InetSocketAddress remoteAddress = null;

            try {
                if (channel.isConnectionPending()) {
                    while (!channel.finishConnect()) {
                        Thread.sleep(100);
                    }
                }
                System.out.println("NetworkManager完成与服务端的连接的建立......");

                remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
                String hostName = remoteAddress.getHostName();
                //初始化发送网络请求的队列
                waitingRequests.put(hostName, new ConcurrentLinkedQueue<>());
                //标记该连接已建立完成
                connectState.put(hostName, CONNECTED);
                connects.put(hostName, key);
            } catch (Exception e) {
                e.printStackTrace();
                if (remoteAddress != null) {
                    connectState.put(remoteAddress.getHostName(), DISCONNECTED);
                }
            }
        }

        /**
         * 发送请求
         *
         * @param key     关注事件的key
         * @param channel 客户端的channel
         */
        private void sendRequest(SelectionKey key, SocketChannel channel) {
            InetSocketAddress remoteAddress = null;
            try {
                remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
                String hostName = remoteAddress.getHostName();

                //获取要发送到这台机器的请求的数据
                NetworkRequest networkRequest = toSendRequests.get(hostName);
                ByteBuffer buffer = networkRequest.getBuffer();

                //写数据
                channel.write(buffer);
                while (buffer.hasRemaining()) {
                    //如果数据没写完，继续写
                    channel.write(buffer);
                }

                System.out.println("本次请求发送完毕......");
                key.interestOps(SelectionKey.OP_READ);
            } catch (Exception e) {
                e.printStackTrace();
                //1.不再关注写事件
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                if (remoteAddress != null) {
                    String hostName = remoteAddress.getHostName();
                    NetworkRequest request = toSendRequests.get(hostName);
                    NetworkResponse response = new NetworkResponse();
                    response.setHostname(hostName);
                    response.setIp(request.getIp());
                    response.setRequestId(request.getId());
                    response.setError(true);
                    //2.是否需要读取响应结果
                    if (request.isNeedResponse()) {
                        finishedResponses.put(request.getId(), response);
                    } else {
                        //3.回调自定义方法
                        if (request.getCallback() != null) {
                            request.getCallback().process(response);
                        }
                        //4.删除相关的缓存
                        toSendRequests.remove(hostName);
                    }
                }
            }

        }

        /**
         * 读取响应结果
         *
         * @param key     关注事件的key
         * @param channel 客户端的channel
         */
        private void readResponse(SelectionKey key, SocketChannel channel) throws IOException {
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            String hostName = remoteAddress.getHostName();
            NetworkRequest request = toSendRequests.get(hostName);
            NetworkResponse response = null;
            if (NetworkRequest.REQUEST_SEND_FILE.equals(request.getRequestType())) {
                response = readSendFileResponse(request.getId(), hostName, request.getIp(), channel);
            }

            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);

            if (request.isNeedResponse()) {
                finishedResponses.put(request.getId(), response);
            } else {
                if (request.getCallback() != null) {
                    request.getCallback().process(response);
                }
                toSendRequests.remove(hostName);
            }
        }

        /**
         * 读取上传文件的响应
         *
         * @param requestId 请求id
         * @param hostName  DataNode的主机名
         * @param channel   DataNode客户端连接
         * @return
         */
        private NetworkResponse readSendFileResponse(String requestId, String hostName, String ip, SocketChannel channel) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            channel.read(buffer);

            buffer.flip();

            NetworkResponse response = new NetworkResponse();
            response.setHostname(hostName);
            response.setIp(ip);
            response.setRequestId(requestId);
            response.setBuffer(buffer);
            response.setError(false);
            return response;
        }
    }

    /**
     * 请求超时检测线程
     */
    class RequestTimeoutCheckThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    long now = System.currentTimeMillis();
                    for (NetworkRequest request : toSendRequests.values()) {
                        if (now - request.getSendTime() > REQUEST_TIMEOUT) {
                            String hostname = request.getHostname();
                            NetworkResponse response = new NetworkResponse();
                            response.setError(true);
                            response.setRequestId(request.getId());
                            response.setHostname(hostname);
                            response.setIp(request.getIp());

                            if (request.isNeedResponse()) {
                                finishedResponses.put(request.getId(), response);
                            } else {
                                if (request.getCallback() != null) {
                                    request.getCallback().process(response);
                                }
                                toSendRequests.remove(hostname);
                            }
                        }
                    }
                    Thread.sleep(REQUEST_TIMEOUT_CHECK_INTERVAL);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


}
