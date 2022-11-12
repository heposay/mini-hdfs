package com.hepo.dfs.namenode.server;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.hepo.dfs.namenode.server.NameNodeConfig.*;


/**
 * Description: 这个组件，就是负责管理集群里的所有的datanode的
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-05-27 10:33
 *
 * @author linhaibo
 */
public class DataNodeManager {

    /**
     * 内存中维护的datanode列表
     */
    private final Map<String, DataNodeInfo> dataNodeInfoMap = new ConcurrentHashMap<>();

    private FSNamesystem namesystem;

    public void setNamesystem(FSNamesystem namesystem) {
        this.namesystem = namesystem;
    }

    public DataNodeManager() {
        DataNodeAliveMonitor dataNodeAliveMonitor = new DataNodeAliveMonitor();
        dataNodeAliveMonitor.setDaemon(true);
        dataNodeAliveMonitor.setName("DataNodeAliveMonitor-");
        dataNodeAliveMonitor.start();
    }


    public DataNodeInfo getDataNodeInfo(String ip, String hostname) {
        return dataNodeInfoMap.get(ip + StringPoolConstant.DASH + hostname);
    }

    public DataNodeInfo getDataNodeInfo(String id) {
        return dataNodeInfoMap.get(id);
    }

    /**
     * 对datanode进行注册
     */
    public Boolean register(String ip, String hostname, Integer uploadServerPort) {
        String key = ip + StringPoolConstant.DASH + hostname;
        if (dataNodeInfoMap.containsKey(key)) {
            System.out.println("dataNodeInfoMap已经存在该信息，不进行重复注册");
            return false;
        }

        DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, hostname, uploadServerPort);
        dataNodeInfoMap.put(key, dataNodeInfo);
        System.out.println("DataNode注册：ip=" + ip + ",hostname=" + hostname + ", uploadServerPort=" + uploadServerPort);
        return true;
    }

    /**
     * datanode进行心跳
     */
    public Boolean heartbeat(String ip, String hostname) {
        DataNodeInfo dataNodeInfo = dataNodeInfoMap.get(ip + StringPoolConstant.DASH + hostname);
        if (dataNodeInfo == null) {
            // 这个时候就需要指示DataNode重新注册以及全量上报
            System.out.println("DataNode信息为空，心跳检测失败，需要重新注册.......");
            return false;
        }
        dataNodeInfo.setLatestHeartbeatTime(System.currentTimeMillis());
        return true;
    }

    /**
     * 设置一个DataNode的存储数据的大小
     *
     * @param ip          ip地址
     * @param hostname    主机名
     * @param storageSize 存储大小
     */
    public void setStorageSize(String ip, String hostname, long storageSize) {
        DataNodeInfo dataNodeInfo = dataNodeInfoMap.get(ip + StringPoolConstant.DASH + hostname);
        dataNodeInfo.setStoredDataSize(storageSize);
    }

    /**
     * 分配副本对应的数据节点
     *
     * @param fileSize 文件大小
     * @return 数据节点集合
     */
    public List<DataNodeInfo> getAllocateDataNodes(long fileSize) {
        synchronized (this) {
            // 取出来所有的datanode，并且按照已经存储的数据大小来排序
            List<DataNodeInfo> datanodeList = new ArrayList<>(dataNodeInfoMap.values());
            Collections.sort(datanodeList);

            // 选择存储数据最少的头两个datanode出来
            List<DataNodeInfo> selectedDatanodes = new ArrayList<>();
            if (datanodeList.size() > DATANODE_DUPLICATE) {
                for (int i = 0; i < DATANODE_DUPLICATE; i++) {
                    selectedDatanodes.add(datanodeList.get(i));
                    //记录该节点已经存储数据的大小
                    selectedDatanodes.get(i).addStoredDataSize(fileSize);
                }
            } else {
                selectedDatanodes.addAll(datanodeList);
            }
            return selectedDatanodes;
        }
    }

    /**
     * 重新分配数据节点
     *
     * @param fileSize           文件大小
     * @param excludedDataNodeId 不包含该节点
     * @return
     */
    public DataNodeInfo reallocateDataNode(long fileSize, String excludedDataNodeId) {
        synchronized (this) {
            //先得把排除掉的那个数据节点的存储的数据量减少文件的大小
            DataNodeInfo excludedDataNode = dataNodeInfoMap.get(excludedDataNodeId);
            excludedDataNode.setStoredDataSize(-fileSize);
            // 取出来所有的datanode，并且按照已经存储的数据大小来排序
            List<DataNodeInfo> dataNodeList = dataNodeInfoMap.values().stream()
                    .filter(e -> !e.equals(excludedDataNode)).sorted().collect(Collectors.toList());
            //选择存储数据最少的头两个datanode出来
            DataNodeInfo selectedDataNode = null;
            if (dataNodeList.size() >= 1) {
                selectedDataNode = dataNodeList.get(0);
                dataNodeList.get(0).addStoredDataSize(fileSize);
            }
            return selectedDataNode;
        }
    }

    /**
     * 创建重平衡的任务
     */
    public void createRebalanceTasks() {
        synchronized (this) {
            //计算集群的平均值
            long totalStoredDataSize = 0;
            for (DataNodeInfo dataNodeInfo : dataNodeInfoMap.values()) {
                totalStoredDataSize += dataNodeInfo.getStoredDataSize();
            }
            long averageStoredDataSize = totalStoredDataSize / dataNodeInfoMap.size();

            //将集群中的节点区分为两类：迁出节点和迁入节点
            List<DataNodeInfo> sourceDataNodes = new ArrayList<>();
            List<DataNodeInfo> destDataNodes = new ArrayList<>();

            for (DataNodeInfo dataNodeInfo : dataNodeInfoMap.values()) {
                if (dataNodeInfo.getStoredDataSize() > averageStoredDataSize) {
                    sourceDataNodes.add(dataNodeInfo);
                }
                if (dataNodeInfo.getStoredDataSize() < averageStoredDataSize) {
                    destDataNodes.add(dataNodeInfo);
                }
            }

            //为迁入节点生成复制的任务，为迁出节点生成删除的任务，删除任务统一放到24小时之后执行
            List<RemoveReplicateTask> removeReplicateTasks = new ArrayList<>();
            for (DataNodeInfo sourceDataNode : sourceDataNodes) {
                long toRemoveDataSize = sourceDataNode.getStoredDataSize() - averageStoredDataSize;
                for (DataNodeInfo destDataNode : destDataNodes) {
                    //直接一次性放到一台机器就可以了
                    if (destDataNode.getStoredDataSize() + toRemoveDataSize <= averageStoredDataSize) {
                        createRebalanceTasks(sourceDataNode, destDataNode, removeReplicateTasks, toRemoveDataSize);
                        break;
                    }
                    //只能放部分数据到这一台机器
                    else if (destDataNode.getStoredDataSize() < averageStoredDataSize) {
                        long maxRemoveDataSize = averageStoredDataSize - destDataNode.getStoredDataSize();
                        long removedDataSize = createRebalanceTasks(sourceDataNode, destDataNode, removeReplicateTasks, maxRemoveDataSize);
                        toRemoveDataSize -= removedDataSize;
                    }
                }
            }
            //交给一个延迟线程去24小时之后执行删除副本的任务
            new DelayRemoveReplicaThread(removeReplicateTasks).start();
        }
    }

    /**
     * 创建重平衡的任务
     *
     * @param sourceDataNode       源节点
     * @param destDataNode         目标节点
     * @param removeReplicateTasks 要删除副本的任务
     * @param maxRemoveDataSize    要删除的数据大小
     * @return
     */
    private long createRebalanceTasks(DataNodeInfo sourceDataNode,
                                      DataNodeInfo destDataNode,
                                      List<RemoveReplicateTask> removeReplicateTasks,
                                      long maxRemoveDataSize) {
        List<String> files = namesystem.getFilesByDataNode(sourceDataNode.getIp(), sourceDataNode.getHostname());

        long removedDataSize = 0;
        for (String file : files) {
            String filename = file.split("_")[0];
            long fileLength = Long.parseLong(file.split("_")[1]);
            if (removedDataSize + fileLength >= maxRemoveDataSize) {
                break;
            }

            // 为这个文件生成复制任务
            ReplicateTask replicateTask = new ReplicateTask(filename, fileLength, sourceDataNode, destDataNode);
            destDataNode.addReplicateTask(replicateTask);
            destDataNode.addStoredDataSize(fileLength);

            // 为这个文件生成删除任务
            namesystem.removeReplicaFromDataNode(sourceDataNode.getId(), file);
            RemoveReplicateTask removeReplicateTask = new RemoveReplicateTask(filename, sourceDataNode);
            removeReplicateTasks.add(removeReplicateTask);
            sourceDataNode.addStoredDataSize(-fileLength);
            removedDataSize += fileLength;
        }
        return removedDataSize;
    }

    /**
     * 延迟删除副本的线程
     */
    static class DelayRemoveReplicaThread extends Thread {
        private final List<RemoveReplicateTask> removeReplicateTasks;

        public DelayRemoveReplicaThread(List<RemoveReplicateTask> removeReplicateTasks) {
            this.removeReplicateTasks = removeReplicateTasks;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    long now = System.currentTimeMillis();
                    if (now - start > REBALANCE_REMOVE_TASK_INTERVAL) {
                        for (RemoveReplicateTask removeReplicateTask : removeReplicateTasks) {
                            removeReplicateTask.getDatanode().addRemoveReplicateTask(removeReplicateTask);
                        }
                        break;
                    }
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * datanode是否存活的监控线程
     */
    @SuppressWarnings("InfiniteLoopStatement")
    class DataNodeAliveMonitor extends Thread {

        @Override
        public void run() {
            try {
                while (true) {
                    List<DataNodeInfo> toRemoveDatanodes = new ArrayList<>();
                    Iterator<DataNodeInfo> iterator = dataNodeInfoMap.values().iterator();
                    DataNodeInfo dataNodeInfo;
                    while (iterator.hasNext()) {
                        //遍历所有的datanode节点的心跳时间，如果心跳时间超过90秒没有更新，说明该节点已经离线，则把该服务摘除
                        dataNodeInfo = iterator.next();
                        if (System.currentTimeMillis() - dataNodeInfo.getLatestHeartbeatTime() > HEARTBEAT_LAST_EXPIRATION_TIME) {
                            toRemoveDatanodes.add(dataNodeInfo);
                        }
                    }
                    if (!toRemoveDatanodes.isEmpty()) {
                        for (DataNodeInfo toRemoveDatanode : toRemoveDatanodes) {
                            System.out.println("数据节点【" + toRemoveDatanode + "】宕机，需要 进行副本复制......");
                            createReplicateTask(toRemoveDatanode);
                            dataNodeInfoMap.remove(toRemoveDatanode.getIp() + StringPoolConstant.DASH + toRemoveDatanode.getHostname());

                            System.out.println("从内存数据结构中删除掉这个数据节点，" + dataNodeInfoMap);
                            // 删除掉这个数据结构
                            namesystem.removeDeadDataNode(toRemoveDatanode);
                        }
                    }
                    Thread.sleep(HEARTBEAT_CHECK_INTERVAL_TIME);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建丢失副本的复制任务
     *
     * @param deadDatanode 宕机的DataNode
     */
    private void createReplicateTask(DataNodeInfo deadDatanode) {
        synchronized (this) {
            List<String> files = namesystem.getFilesByDataNode(deadDatanode.getIp(), deadDatanode.getHostname());
            for (String file : files) {
                String filename = file.split(StringPoolConstant.UNDERLINE)[0];
                Long fileLength = Long.valueOf(file.split(StringPoolConstant.UNDERLINE)[1]);
                // 获取这个复制任务的源头数据节点
                DataNodeInfo sourceDataNode = namesystem.getReplicateSource(filename, deadDatanode);
                // 复制任务的目标数据节点，第一，不能是已经死掉的节点 ；第二，不能是已经有这个副本的节点
                DataNodeInfo destDatanode = allocateReplicateDataNode(fileLength, sourceDataNode, deadDatanode);
                // 创建复制副本的任务
                ReplicateTask replicateTask = new ReplicateTask(
                        filename, fileLength, sourceDataNode, destDatanode);

                // 将复制任务放到目标数据节点的任务队列里去
                destDatanode.addReplicateTask(replicateTask);

                System.out.println("为目标数据节点生成一个副本复制任务，" + replicateTask);

            }
        }

    }

    /**
     * 分配用来复制副本的数据节点
     *
     * @param fileLength     文件长度
     * @param sourceDataNode 源副本所在的Datanode
     * @param deadDatanode   宕机的Datanode
     * @return 目标Datanode
     */
    private DataNodeInfo allocateReplicateDataNode(Long fileLength, DataNodeInfo sourceDataNode, DataNodeInfo deadDatanode) {
        synchronized (this) {
            List<DataNodeInfo> dataNodeInfoList = new ArrayList<>();
            for (DataNodeInfo dataNodeInfo : dataNodeInfoMap.values()) {
                if (!dataNodeInfo.equals(sourceDataNode) && !dataNodeInfo.equals(deadDatanode)) {
                    dataNodeInfoList.add(dataNodeInfo);
                }
            }
            //排序，找出副本存储最少的Datanode节点
            Collections.sort(dataNodeInfoList);

            DataNodeInfo selectedDatanode = null;
            if (!dataNodeInfoList.isEmpty()) {
                selectedDatanode = dataNodeInfoList.get(0);
                dataNodeInfoList.get(0).addStoredDataSize(fileLength);
            }
            return selectedDatanode;
        }
    }
}
