package com.hepo.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Description: 拉取namenode上面的editlog组件
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-29 14:54
 *
 * @author linhaibo
 */
public class EditsLogFetcher extends Thread {

    private BackupNode backupNode;
    private BackupNodeRpcClient backupNodeRpcClient;
    private FSNamesystem namesystem;

    /**
     * 默认拉取日志的数目
     */
    private static final int BACKUP_NODE_FETCH_SIZE = 10;


    public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.backupNodeRpcClient = new BackupNodeRpcClient();
    }

    @Override
    public void run() {
        while (backupNode.isRunning()) {
            try {
                JSONArray editsLogs = backupNodeRpcClient.fetchEditsLog();
                if (editsLogs.size() == 0) {
                    //System.out.println("没有拉取到任何一条editslog，等待1秒后继续尝试拉取");
                    Thread.sleep(1000);
                    continue;
                }

                if (editsLogs.size() < BACKUP_NODE_FETCH_SIZE) {
                    System.out.println("内存缓冲区数据少于10条，等待1秒后继续尝试拉取");
                    Thread.sleep(1000);
                    continue;
                }
                for (int i = 0; i < editsLogs.size(); i++) {
                    JSONObject editsLog = editsLogs.getJSONObject(i);
                    System.out.println("拉取到一条editslog：" + editsLog.toJSONString());
                    String op = editsLog.getString("OP");
                    if (op.equals("MKDIR")) {
                        String path = editsLog.getString("PATH");
                        namesystem.mkdir(path);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
