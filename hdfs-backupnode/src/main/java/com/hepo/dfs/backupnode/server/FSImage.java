package com.hepo.dfs.backupnode.server;

/**
 * Description: fsImage对象
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-06-29 22:14
 *
 * @author linhaibo
 */
public class FSImage {
    private Long maxTxid;
    private String FSImageJson;

    public FSImage(Long maxTxid, String FSImageJson) {
        this.maxTxid = maxTxid;
        this.FSImageJson = FSImageJson;
    }

    public Long getMaxTxid() {
        return maxTxid;
    }

    public void setMaxTxid(Long maxTxid) {
        this.maxTxid = maxTxid;
    }

    public String getFSImageJson() {
        return FSImageJson;
    }

    public void setFSImageJson(String FSImageJson) {
        this.FSImageJson = FSImageJson;
    }
}
