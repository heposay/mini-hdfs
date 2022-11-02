package com.hepo.dfs.backupnode.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Description: fsimage文件的checkpoint组件
 * Project:  mini-hdfs
 * CreateDate: Created in 2022-06-29 21:53
 *
 * @author linhaibo
 */
public class FSImageCheckpointer extends Thread {

    /**
     * checkpoint的时间间隔
     */
    private static final Integer CHECKPOINT_INTERVAL = 30 * 1000;

    private final BackupNode backupNode;

    private final FSNamesystem namesystem;

    private final BackupNodeRpcClient namenode;

    private String lastFSImageFile = "";

    public FSImageCheckpointer(BackupNode backupNode, FSNamesystem namesystem, BackupNodeRpcClient namenode) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.namenode = namenode;
    }

    @Override
    public void run() {
        System.out.println("fsimage checkpoint定时调度线程启动......");
        while (backupNode.isRunning()) {
            try {
                Thread.sleep(CHECKPOINT_INTERVAL);
                if (namesystem.getSyncedTxid() != 0) {
                    // 就可以触发这个checkpoint操作，去把内存里的数据写入磁盘就可以了
                    System.out.println("BackupNode准备执行checkpoint操作，写入fsimage文件......");
                    doCheckpoint();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 将fsImage持久化到磁盘去
     */
    private void doCheckpoint() throws IOException {
        //获取fsImage文件
        FSImage fsImage = namesystem.getFSImage();
        //删除上次文件
        removeLastFsimageFile();
        //写fsImage文件到磁盘
        writeFsImageFile(fsImage);
        //上传fsImage文件到NameNode
        uploadFsImageFile(fsImage);
        //更新checkpoint txid
        updateCheckpointTxid(fsImage);
    }

    /**
     * 删除上一个fsimage磁盘文件
     */
    private void removeLastFsimageFile() {
        File file = new File(lastFSImageFile);
        if (file.exists()) {
            file.delete();
        }
    }


    /**
     *  写fsImage文件到磁盘
     * @param fsImage fsImage文件
     * @throws IOException
     */
    private void writeFsImageFile(FSImage fsImage) throws IOException{
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFSImageJson().getBytes());
        System.out.println("开始执行doCheckpoint操作，maxTxid：" + fsImage.getMaxTxid());
        //定义要写的目录路径
        String fsimageFilePath = "/Users/linhaibo/Documents/tmp/backupnode/fsimage-" + fsImage.getMaxTxid() + ".meta";

        lastFSImageFile = fsimageFilePath;

        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel channel = null;
        try {
            file = new RandomAccessFile(fsimageFilePath, "rw");
            out = new FileOutputStream(file.getFD());
            channel = out.getChannel();

            channel.write(buffer);
            channel.force(false);
        } finally {
            if (channel != null) {
                channel.close();
            }
            if (out != null) {
                out.close();
            }
            if (file != null) {
                file.close();
            }
        }

    }

    /**
     * 上传fsImage文件到NameNode
     * @param fsImage  fsImage文件
     */
    private void uploadFsImageFile(FSImage fsImage) {
        FSImageUploader fsImageUploader = new FSImageUploader(fsImage);
        fsImageUploader.start();
    }


    /**
     * 更新checkpoint txid
     * @param fsImage fsImage文件
     */
    private void updateCheckpointTxid (FSImage fsImage) {
        namenode.updateCheckpointTxid(fsImage.getMaxTxid());
    }


}
