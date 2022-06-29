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

    public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.backupNodeRpcClient = new BackupNodeRpcClient();
    }

    @Override
    public void run() {
        while (backupNode.isRunning()) {
            JSONArray editsLogs = backupNodeRpcClient.fetchEditsLog();
            for (int i = 0; i < editsLogs.size(); i++) {
                JSONObject editsLogJson = editsLogs.getJSONObject(i);
                String op = editsLogJson.getString("OP");
                if (op.equals("MKDIR")) {
                    String path = editsLogJson.getString("PATH");
                    namesystem.mkdir(path);
                }
            }
        }
    }
}
