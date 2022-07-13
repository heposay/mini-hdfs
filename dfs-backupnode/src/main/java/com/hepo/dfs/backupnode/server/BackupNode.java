package com.hepo.dfs.backupnode.server;

/**
 * Description:
 * Project:  hdfs-study
 * CreateDate: Created in 2022-06-29 14:53
 *
 * @author linhaibo
 */
public class BackupNode {

    private volatile Boolean isRunning = true;
    private FSNamesystem namesystem;

    private BackupNodeRpcClient namenode;

    public static void main(String[] args) throws InterruptedException {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
        //backupNode.run();
    }

    public void start() {
        EditsLogFetcher fetcher = new EditsLogFetcher(this, namesystem);
        fetcher.start();

        FSImageCheckpointer checkpointer = new FSImageCheckpointer(this, namesystem, namenode);
        checkpointer.start();
    }

    public void init() {
        this.namesystem = new FSNamesystem();
        this.namenode  = new BackupNodeRpcClient();
    }


    public void run() throws InterruptedException {
        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    public Boolean isRunning() {
        return isRunning;
    }
}
