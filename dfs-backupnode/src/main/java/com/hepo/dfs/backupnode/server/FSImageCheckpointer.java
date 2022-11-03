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
    private static final Integer CHECKPOINT_INTERVAL = 60 * 1000;

    private final BackupNode backupNode;

    private final FSNamesystem namesystem;

    private final BackupNodeRpcClient namenode;

    private String lastFSImageFile = "";

    private long checkpointTime = System.currentTimeMillis();

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
                if (!namesystem.isFinishedRecover()) {
                    System.out.println("当前还没完成元数据恢复，不进行checkpoint......");
                    Thread.sleep(1000);
                    continue;
                }
                if (lastFSImageFile.equals("")) {
                    this.lastFSImageFile = namesystem.getCheckpointFile();
                }
                long now = System.currentTimeMillis();
                if (now - checkpointTime > CHECKPOINT_INTERVAL) {
                    if (!namenode.isNamenodeRunning()) {
                        System.out.println("namenode当前无法访问，不执行checkpoint......");
                        Thread.sleep(3000);
                        continue;
                    }
                    // 就可以触发这个checkpoint操作，去把内存里的数据写入磁盘就可以了
                    System.out.println("BackupNode准备执行checkpoint操作，写入fsimage文件......");
                    doCheckpoint();
                    System.out.println("完成checkpoint操作......");
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
        //持久化checkpoint信息
        saveCheckpointInfo(fsImage);
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

    /**
     * 持久化checkpoint信息
     * @param fsImage  fsImage文件
     */
    private void saveCheckpointInfo(FSImage fsImage) {
        String path = "/Users/linhaibo/Documents/tmp/backupnode/checkpoint-info.meta";
        RandomAccessFile raf = null;
        FileOutputStream fos = null;
        FileChannel channel = null;

        try{
            //删除已存在文件
            File file = new File(path);
            if (file.exists()) {
                file.delete();
            }

            long now = System.currentTimeMillis();
            this.checkpointTime = now;
            Long checkpointTxid = fsImage.getMaxTxid();
            //构建buffer
            ByteBuffer buffer = ByteBuffer.wrap((now +
                    StringPoolConstant.UNDERLINE +
                    checkpointTxid +
                    StringPoolConstant.UNDERLINE +
                    lastFSImageFile
            ).getBytes());

            raf = new RandomAccessFile(path, "rw");
            fos = new FileOutputStream(raf.getFD());
            channel = fos.getChannel();

            channel.write(buffer);
            channel.force(false);

            System.out.println("checkpoint信息持久化到磁盘文件......");

        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
