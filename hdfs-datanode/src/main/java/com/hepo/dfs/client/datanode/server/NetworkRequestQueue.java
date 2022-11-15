package com.hepo.dfs.client.datanode.server;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 13:41
 *
 * @author linhaibo
 */
public class NetworkRequestQueue {
    private static volatile NetworkRequestQueue instance = null;

    private NetworkRequestQueue() {
    }

    public static NetworkRequestQueue getInstance() {
        if (instance == null) {
            synchronized (NetworkRequest.class) {
                if (instance == null) {
                    instance = new NetworkRequestQueue();
                }
            }
        }
        return instance;
    }

    private final ConcurrentLinkedQueue<NetworkRequest> requestQueue = new ConcurrentLinkedQueue<>();

    public void offer(NetworkRequest request) {
        requestQueue.offer(request);
    }

    public NetworkRequest poll() {
        return requestQueue.poll();
    }
}
