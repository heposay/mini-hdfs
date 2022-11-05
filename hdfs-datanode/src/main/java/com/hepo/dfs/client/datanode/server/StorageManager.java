package com.hepo.dfs.client.datanode.server;

import java.io.File;

/**
 * Description: 磁盘存储管理组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/5 13:44
 *
 * @author linhaibo
 */
public class StorageManager {

    /**
     * 获取存储信息
     *
     * @return 存储信息
     */
    public StorageInfo getStorageInfo() {
        StorageInfo storageInfo = new StorageInfo();
        File dataDir = new File(DataNodeConfig.DATA_DIR);
        File[] children = dataDir.listFiles();
        if (children == null || children.length == 0) {
            return null;
        }
        for (File child : children) {
            scanFiles(child, storageInfo);
        }
        return storageInfo;
    }

    /**
     * 扫描文件
     *
     * @param dir       子目录
     * @param storageInfo 存储信息
     */
    private void scanFiles(File dir, StorageInfo storageInfo) {
        if (dir.isFile()) {
            String path = dir.getPath();
            path = path.substring(DataNodeConfig.DATA_DIR.length());
            path.replace("\\", "/");
            storageInfo.addFilename(path);
            storageInfo.addStorageSize(dir.length());
            return;
        }
        File[] children = dir.listFiles();
        if (children == null || children.length == 0) {
            return;
        }
        for (File child : children) {
            scanFiles(child, storageInfo);
        }
    }
}
