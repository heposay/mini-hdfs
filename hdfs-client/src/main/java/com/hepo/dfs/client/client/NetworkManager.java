package com.hepo.dfs.client.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
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
     * 多路复用器
     */
    private Selector selector;
    /**
     * 每个数据节点的连接状态
     */
    private Map<String, Integer> connectState;

    private ConcurrentLinkedQueue<Host> waitingConnectHosts;

    public NetworkManager() {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.connectState = new ConcurrentHashMap<>();
        waitingConnectHosts = new ConcurrentLinkedQueue<>();

        //启动后台线程
        new NetworkPollThread().start();
    }

    /**
     * 尝试建立连接
     *
     * @param hostname         DataNode的主机名
     * @param uploadServerPort DataNode的上传端口
     */
    public void maybeConnect(String hostname, Integer uploadServerPort) throws Exception {
        synchronized (this) {
            if (!connectState.containsKey(hostname)) {
                connectState.put(hostname, CONNECTING);
                waitingConnectHosts.offer(new Host(hostname, uploadServerPort));
            }

            while (CONNECTING.equals(connectState.get(hostname))) {
                //如果连接还没建立完成，释放锁等待100ms
                wait(100);
            }
        }
    }

    /**
     * 处理网络连接的核心线程
     */
    class NetworkPollThread extends Thread{
        @Override
        public void run() {
            tryConnect();
        }

        /**
         * 尝试与DataNode建立连接
         */
        private void tryConnect() {
            try {
                Host host;
                SocketChannel channel;
                while ((host = waitingConnectHosts.poll()) != null) {
                    channel = SocketChannel.open();
                    channel.configureBlocking(false);
                    channel.connect(new InetSocketAddress(host.hostname, host.uploadServerPort));
                    channel.register(selector, SelectionKey.OP_CONNECT);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 代表一台数据节点机器
     */
    class Host{
        String hostname;
        Integer uploadServerPort;
        public Host(String hostname, Integer uploadServerPort) {
            this.hostname = hostname;
            this.uploadServerPort = uploadServerPort;
        }
    }
}
