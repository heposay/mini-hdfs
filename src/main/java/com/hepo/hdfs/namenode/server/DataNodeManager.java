package com.hepo.hdfs.namenode.server;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: 这个组件，就是负责管理集群里的所有的datanode的
 * Project:  hdfs-study
 * CreateDate: Created in 2022-05-27 10:33
 *
 * @author linhaibo
 */
public class DataNodeManager {

    /**
     * 内存中维护的datanode列表
     */
    private List<DataNodeInfo> dataList = new ArrayList<>();

    /**
     * 对datanode进行注册
     *
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean register(String ip, String hostname) {
        DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, hostname);
        dataList.add(dataNodeInfo);
        return true;
    }
}
