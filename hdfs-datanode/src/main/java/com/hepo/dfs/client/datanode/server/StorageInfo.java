package com.hepo.dfs.client.datanode.server;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:存储信息
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/5 10:45
 *
 * @author linhaibo
 */
public class StorageInfo {
    private List<String> filenames = new ArrayList<>();
    private Long storageSize = 0L;

    public List<String> getFilenames() {
        return filenames;
    }

    public void setFilenames(List<String> filenames) {
        this.filenames = filenames;
    }

    public Long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(Long storageSize) {
        this.storageSize = storageSize;
    }

    public void addFilename(String filename) {
        filenames.add(filename);
    }

    public void addStorageSize(long storageSize) {
        this.storageSize += storageSize;
    }

    @Override
    public String toString() {
        return "StorageInfo{" +
                "filenames=" + filenames +
                ", storageSize=" + storageSize +
                '}';
    }
}
