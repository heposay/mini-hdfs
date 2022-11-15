package com.hepo.dfs.client.datanode.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Description: 网络响应队列
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 15:45
 *
 * @author linhaibo
 */
public class NetworkResponseQueue {
    private static volatile NetworkResponseQueue instance = null;

    private NetworkResponseQueue() {
    }

    public static NetworkResponseQueue getInstance() {
        if (instance == null) {
            synchronized (NetworkResponseQueue.class) {
                if (instance == null) {
                    instance = new NetworkResponseQueue();
                }
            }
        }
        return instance;
    }

    /**
     * 存放网络响应队列
     */
    private final Map<Integer, ConcurrentLinkedQueue<NetworkResponse>> responseQueue = new HashMap<>();


    public void initReponseQueue(Integer processorId) {
        ConcurrentLinkedQueue<NetworkResponse> queue = new ConcurrentLinkedQueue<>();
        responseQueue.put(processorId, queue);
    }

    public void offer(Integer processorId, NetworkResponse response) {
        responseQueue.get(processorId).offer(response);
    }

    public NetworkResponse poll(Integer processorId) {
        return responseQueue.get(processorId).poll();
    }
}
