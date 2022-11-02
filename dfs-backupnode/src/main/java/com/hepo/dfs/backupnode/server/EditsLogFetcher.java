package com.hepo.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Description: 拉取namenode上面的editlog组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-06-29 14:54
 *
 * @author linhaibo
 */
public class EditsLogFetcher extends Thread {

    private final BackupNode backupNode;
    private final BackupNodeRpcClient backupNodeRpcClient;
    private final FSNamesystem namesystem;

    /**
     * 默认拉取日志的数目
     */
    private static final int BACKUP_NODE_FETCH_SIZE = 10;


    public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem, BackupNodeRpcClient backupNodeRpcClient) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.backupNodeRpcClient = backupNodeRpcClient;
    }

    @Override
    public void run() {
        System.out.println("editsLogFetcher 定时拉取EditLog线程启动....");
        while (backupNode.isRunning()) {
            try {
                //从NameNode同步EditLog日志
                JSONArray editsLogs = backupNodeRpcClient.fetchEditsLog(namesystem.getSyncedTxid());
                //如果没拉取到的数据，睡眠1秒钟
                if (editsLogs.size() == 0) {
                    Thread.sleep(1000);
                    continue;
                }
                for (int i = 0; i < editsLogs.size(); i++) {
                    JSONObject editsLog = editsLogs.getJSONObject(i);
                    System.out.println("拉取到一条editslog：" + editsLog.toJSONString());
                    String op = editsLog.getString("OP");
                    if (op.equals(EditLogOperation.MKDIR)) {
                        String path = editsLog.getString(EditLogOperation.PATH);
                        namesystem.mkdir(editsLog.getLong("txid"), path);
                    } else if (op.equals(EditLogOperation.CREATE)) {
                        String path = editsLog.getString(EditLogOperation.PATH);
                        namesystem.create(editsLog.getLong("txid"), path);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
