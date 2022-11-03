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
    private final BackupNodeRpcClient namenode;
    private final FSNamesystem namesystem;

    /**
     * 默认拉取日志的数目
     */
    private static final int BACKUP_NODE_FETCH_SIZE = 10;


    public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem, BackupNodeRpcClient namenode) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.namenode = namenode;
    }

    @Override
    public void run() {
        System.out.println("editsLogFetcher 定时拉取EditLog线程启动....");
        while (backupNode.isRunning()) {
            try {
                if (!namesystem.isFinishedRecover()) {
                    System.out.println("当前还没完成元数据恢复，不进行editlog同步......");
                    Thread.sleep(1000);
                    continue;
                }
                if (!namenode.isNamenodeRunning()) {
                    System.out.println("NameNode当前处于故障阶段，不进行editlog同步.....");
                    Thread.sleep(1000);
                    continue;
                }
                //获取上次同步的syncedTxid
                long syncedTxid = namesystem.getSyncedTxid();
                //从NameNode同步EditLog日志
                JSONArray editsLogs = namenode.fetchEditsLog(syncedTxid);
                //如果没拉取到的数据，睡眠1秒钟
                if (editsLogs.size() == 0) {
                    Thread.sleep(1000);
                    continue;
                }
                if (editsLogs.size() < BACKUP_NODE_FETCH_SIZE) {
                    Thread.sleep(1000);
                    System.out.println("拉取到的edits log不足10条数据，等待1秒后再次继续去拉取");
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
