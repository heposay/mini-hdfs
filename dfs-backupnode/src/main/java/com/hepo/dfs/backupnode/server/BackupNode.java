package com.hepo.dfs.backupnode.server;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-06-29 14:53
 *
 * @author linhaibo
 */
public class BackupNode {

    private volatile Boolean isRunning = true;
    private FSNamesystem namesystem;

    private BackupNodeRpcClient namenode;

    public static void main(String[] args) {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
    }

    public void start() {
        EditsLogFetcher fetcher = new EditsLogFetcher(this, namesystem, namenode);
        fetcher.start();

        FSImageCheckpointer checkpointer = new FSImageCheckpointer(this, namesystem, namenode);
        checkpointer.start();
    }

    public void init() {
        this.namesystem = new FSNamesystem();
        this.namenode = new BackupNodeRpcClient();
    }

    public Boolean isRunning() {
        return isRunning;
    }
}
