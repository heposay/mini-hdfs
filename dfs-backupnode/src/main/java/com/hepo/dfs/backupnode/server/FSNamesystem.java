package com.hepo.dfs.backupnode.server;

/**
 * Description: 负责管理组件的所有元数据
 * Project:  hdfs_study
 * CreateDate: Created in 2022-04-22 09:46
 *
 * @author linhaibo
 */
public class FSNamesystem {

    /**
     * 负责管理内存中文件目录树的组件
     */
    private FSDirectory directory;


    /**
     * 初始化组件
     */
    public FSNamesystem() {
        directory = new FSDirectory();
    }

    /**
     * 创建目录
     *
     * @param path 文件路径
     * @return 是否成功
     */
    public Boolean mkdir(long txid, String path) {
        this.directory.mkdir(txid, path);
        return true;
    }


    public Boolean create(String path) {
        this.directory.create(path);
        return true;
    }


    public FSImage getFSImage() {
        return directory.getFSImage();
    }

    public long getSyncedTxid() {
        return directory.getFSImage().getMaxTxid();
    }
}
