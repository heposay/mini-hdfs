package com.hepo.dfs.backupnode.server;

import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Description: 负责管理组件的所有元数据
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-04-22 09:46
 *
 * @author linhaibo
 */
public class FSNamesystem {

    /**
     * 负责管理内存中文件目录树的组件
     */
    private FSDirectory directory;

    private long checkpointTime;

    private long syncedTxid;

    private String checkpointFile = "";

    private volatile boolean finishedRecover = false;


    /**
     * 初始化组件
     */
    public FSNamesystem() {
        directory = new FSDirectory();
        recoverNamespace();
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

    /**
     * 创建文件
     *
     * @param path 文件路径
     * @return 是否成功
     */
    public Boolean create(long txid, String path) {
        this.directory.create(txid, path);
        return true;
    }

    /**
     * 获取fsimage文件
     */
    public FSImage getFSImage() {
        return directory.getFSImage();
    }


    /**
     * 恢复元数据
     */
    public void recoverNamespace() {
        try {
            loadCheckpointInfo();
            loadFSImage();
            finishedRecover = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 加载checkpoint txid
     */
    private void loadCheckpointInfo() throws IOException {
        FileInputStream fis = null;
        FileChannel channel = null;

        try {
            String path = "/Users/linhaibo/Documents/tmp/backupnode/checkpoint-info.meta";
            File file = new File(path);
            if (!file.exists()) {
                System.out.println("checkpoint info文件不存在，不进行恢复.......");
                return;
            }
            fis = new FileInputStream(path);
            channel = fis.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            // 每次你接受到一个fsimage文件的时候记录一下他的大小，持久化到磁盘上去
            // 每次重启就分配对应空间的大小就可以了
            int count = channel.read(buffer);

            buffer.flip();
            //解析数据
            String checkpointInfo = new String(buffer.array(), 0, count);
            Long checkpointTime= Long.valueOf(checkpointInfo.split(StringPoolConstant.UNDERLINE)[0]);
            Long syncedTxid= Long.valueOf(checkpointInfo.split(StringPoolConstant.UNDERLINE)[1]);
            String fsimageFile = checkpointInfo.split(StringPoolConstant.UNDERLINE)[2];

            System.out.println("恢复checkpoint time：" + checkpointTime + ", synced txid: " + syncedTxid + ", fsimage file: " + fsimageFile);
            this.checkpointTime = checkpointTime;
            this.syncedTxid = syncedTxid;
            this.checkpointFile = fsimageFile;

            directory.setMaxTxid(syncedTxid);

        }finally {
            if (fis != null) {
                fis.close();
            }
            if (channel != null) {
                channel.close();
            }
        }

    }

    /**
     * 加载fsimage文件到内存里来进行恢复
     */
    private void loadFSImage() throws IOException {
        FileInputStream fis = null;
        FileChannel channel = null;

        try{
            File file = new File(checkpointFile);
            if (!file.exists()) {
                System.out.println("fsimage文件当前不存在，不进行恢复.......");
                return;
            }
            fis = new FileInputStream(checkpointFile);
            channel = fis.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            int count = channel.read(buffer);

            String fsimageJson = new String(buffer.array(), 0, count);
            System.out.println("BackupNode恢复fsimage文件中的数据：" + fsimageJson);
            FSDirectory.INode dirTree = JSONObject.parseObject(fsimageJson, FSDirectory.INode.class);
            directory.setDirTree(dirTree);
        }finally {
            if (fis != null) {
                fis.close();
            }
            if (channel != null) {
                channel.close();
            }
        }

    }

    public long getCheckpointTime() {
        return checkpointTime;
    }

    public void setCheckpointTime(long checkpointTime) {
        this.checkpointTime = checkpointTime;
    }

    /**
     * 获取当前同步的最大txid
     */
    public long getSyncedTxid() {
        return directory.getFSImage().getMaxTxid();
    }


    public void setSyncedTxid(long syncedTxid) {
        this.syncedTxid = syncedTxid;
    }

    public String getCheckpointFile() {
        return checkpointFile;
    }

    public void setCheckpointFile(String checkpointFile) {
        this.checkpointFile = checkpointFile;
    }

    public boolean isFinishedRecover() {
        return finishedRecover;
    }

    public void setFinishedRecover(boolean finishedRecover) {
        this.finishedRecover = finishedRecover;
    }
}
