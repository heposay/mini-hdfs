package com.hepo.dfs.datanode.server;

/**
 * Description: DataNode启动类
 * Project:  hdfs_study
 * CreateDate: Created in 2022-05-06 14:24
 *
 * @author linhaibo
 */
public class DataNode {

    /**
     * 运行标志
     */
    private volatile Boolean shouldRun;

    /**
     * 负责跟一组NameNode通信的组件
     */
    private NameNodeGroupOfferService offerService;

    /**
     * DataNode初始化
     */
    public void initialize() {
        this.shouldRun = true;
        this.offerService = new NameNodeGroupOfferService();
        this.offerService.start();
    }

    private void run() {
        try {
            while(shouldRun) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DataNode dataNode = new DataNode();
        dataNode.initialize();
        dataNode.run();
    }

}
